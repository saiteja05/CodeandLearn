package runner

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mongodb-ai-bench/internal/config"
	"github.com/mongodb-ai-bench/internal/metrics"
	"github.com/mongodb-ai-bench/internal/pool"
	"github.com/mongodb-ai-bench/internal/workload"
)

type PhaseInfo struct {
	Name               string
	Duration           time.Duration
	TargetVirtualUsers int
	Ramp               string
	StartTime          time.Time
	EndTime            time.Time
}

type VUManager struct {
	mu          sync.Mutex
	activeVUs   map[int]context.CancelFunc
	nextID      int
	poolMgr     *pool.Manager
	collector   *metrics.Collector
	workloadCfg config.WorkloadConfig
	wg          sync.WaitGroup
}

func NewVUManager(poolMgr *pool.Manager, collector *metrics.Collector, workloadCfg config.WorkloadConfig) *VUManager {
	return &VUManager{
		activeVUs:   make(map[int]context.CancelFunc),
		poolMgr:     poolMgr,
		collector:   collector,
		workloadCfg: workloadCfg,
	}
}

func (vm *VUManager) CurrentCount() int {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return len(vm.activeVUs)
}

func (vm *VUManager) ScaleTo(ctx context.Context, target int) {
	vm.mu.Lock()
	current := len(vm.activeVUs)
	vm.mu.Unlock()

	if target > current {
		vm.addVUs(ctx, target-current)
	} else if target < current {
		vm.removeVUs(current - target)
	}
}

func (vm *VUManager) addVUs(ctx context.Context, count int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	for i := 0; i < count; i++ {
		vuID := vm.nextID
		vm.nextID++

		vuCtx, cancel := context.WithCancel(ctx)
		vm.activeVUs[vuID] = cancel

		seed := uint64(vuID) * 6364136223846793005
		msgGen := workload.NewMessageGenerator(vm.workloadCfg, seed)

		vu := workload.NewVirtualUserWithParams(workload.VirtualUserParams{
			ID:                   vuID,
			PoolMgr:              vm.poolMgr,
			Collector:            vm.collector,
			MsgGen:               msgGen,
			ConversationsEnabled: vm.workloadCfg.ConversationsEnabled(),
			MaxHistoryMessages:   vm.workloadCfg.MaxHistoryMessages,
		})

		vm.wg.Add(1)
		go func() {
			defer vm.wg.Done()
			vu.Run(vuCtx)
		}()
	}
}

func (vm *VUManager) removeVUs(count int) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	removed := 0
	for id, cancel := range vm.activeVUs {
		if removed >= count {
			break
		}
		cancel()
		delete(vm.activeVUs, id)
		removed++
	}
}

func (vm *VUManager) StopAll() {
	vm.mu.Lock()
	for id, cancel := range vm.activeVUs {
		cancel()
		delete(vm.activeVUs, id)
	}
	vm.mu.Unlock()
	vm.wg.Wait()
}

type ProgressiveRunner struct {
	phases        []PhaseInfo
	vuManager     *VUManager
	collector     *metrics.Collector
	logger        *slog.Logger
	onPhaseChange func(phase string)
}

func NewProgressiveRunner(
	phases []config.Phase,
	poolMgr *pool.Manager,
	collector *metrics.Collector,
	workloadCfg config.WorkloadConfig,
	onPhaseChange func(phase string),
) (*ProgressiveRunner, error) {
	parsed := make([]PhaseInfo, 0, len(phases))
	for _, p := range phases {
		dur, err := p.ParsedDuration()
		if err != nil {
			return nil, fmt.Errorf("parsing phase %q duration: %w", p.Name, err)
		}
		parsed = append(parsed, PhaseInfo{
			Name:               p.Name,
			Duration:           dur,
			TargetVirtualUsers: p.TargetVirtualUsers,
			Ramp:               p.Ramp,
		})
	}

	return &ProgressiveRunner{
		phases:        parsed,
		vuManager:     NewVUManager(poolMgr, collector, workloadCfg),
		collector:     collector,
		logger:        slog.Default().With("component", "progressive_runner"),
		onPhaseChange: onPhaseChange,
	}, nil
}

func (pr *ProgressiveRunner) Run(ctx context.Context) error {
	for i, phase := range pr.phases {
		select {
		case <-ctx.Done():
			pr.logger.Info("benchmark cancelled, stopping")
			pr.vuManager.StopAll()
			return ctx.Err()
		default:
		}

		pr.logger.Info("starting phase",
			"phase", phase.Name,
			"duration", phase.Duration,
			"target_vus", phase.TargetVirtualUsers,
			"ramp", phase.Ramp,
			"phase_num", i+1,
			"total_phases", len(pr.phases),
		)

		if pr.onPhaseChange != nil {
			pr.onPhaseChange(phase.Name)
		}

		if err := pr.runPhase(ctx, phase); err != nil {
			if ctx.Err() != nil {
				pr.vuManager.StopAll()
				return ctx.Err()
			}
			return fmt.Errorf("phase %q failed: %w", phase.Name, err)
		}

		pr.logger.Info("phase completed",
			"phase", phase.Name,
			"total_ops", pr.collector.TotalOps(),
			"total_errors", pr.collector.TotalErrors(),
		)
	}

	pr.logger.Info("all phases completed, draining virtual users")
	pr.vuManager.StopAll()
	return nil
}

func (pr *ProgressiveRunner) runPhase(ctx context.Context, phase PhaseInfo) error {
	phaseCtx, cancel := context.WithTimeout(ctx, phase.Duration)
	defer cancel()

	startVUs := pr.vuManager.CurrentCount()
	targetVUs := phase.TargetVirtualUsers

	switch phase.Ramp {
	case "none":
		pr.vuManager.ScaleTo(phaseCtx, targetVUs)
		<-phaseCtx.Done()

	case "linear":
		pr.linearRamp(phaseCtx, startVUs, targetVUs, phase.Duration)

	case "step":
		pr.stepRamp(phaseCtx, startVUs, targetVUs, phase.Duration)

	default:
		pr.vuManager.ScaleTo(phaseCtx, targetVUs)
		<-phaseCtx.Done()
	}

	return nil
}

func (pr *ProgressiveRunner) linearRamp(ctx context.Context, startVUs, targetVUs int, duration time.Duration) {
	steps := 20
	stepDuration := duration / time.Duration(steps)
	if stepDuration < time.Second {
		stepDuration = time.Second
		steps = int(duration / time.Second)
	}

	for i := 0; i <= steps; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		progress := float64(i) / float64(steps)
		currentTarget := startVUs + int(float64(targetVUs-startVUs)*progress)
		pr.vuManager.ScaleTo(ctx, currentTarget)

		pr.logger.Info("ramp progress",
			"vus", currentTarget,
			"target", targetVUs,
			"progress_pct", int(progress*100),
		)

		if i < steps {
			select {
			case <-ctx.Done():
				return
			case <-time.After(stepDuration):
			}
		}
	}

	<-ctx.Done()
}

func (pr *ProgressiveRunner) stepRamp(ctx context.Context, startVUs, targetVUs int, duration time.Duration) {
	steps := 10
	stepDuration := duration / time.Duration(steps)

	for i := 1; i <= steps; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		progress := float64(i) / float64(steps)
		currentTarget := startVUs + int(float64(targetVUs-startVUs)*progress)
		pr.vuManager.ScaleTo(ctx, currentTarget)

		pr.logger.Info("step ramp",
			"step", i,
			"vus", currentTarget,
			"target", targetVUs,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(stepDuration):
		}
	}
}

func (pr *ProgressiveRunner) ActiveVUs() int {
	return pr.vuManager.CurrentCount()
}
