package workload

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb-ai-bench/internal/metrics"
	"github.com/mongodb-ai-bench/internal/pool"
)

const opTimeout = 30 * time.Second

type VirtualUser struct {
	id                     int
	userID                 uuid.UUID
	runner                 *ConversationRunner
	msgGen                 *MessageGenerator
	logger                 *slog.Logger
	collector              *metrics.Collector
	conversationsEnabled   bool
}

type VirtualUserParams struct {
	ID                   int
	PoolMgr              *pool.Manager
	Collector            *metrics.Collector
	MsgGen               *MessageGenerator
	ConversationsEnabled bool
}

func NewVirtualUserWithParams(p VirtualUserParams) *VirtualUser {
	return &VirtualUser{
		id:                   p.ID,
		userID:               uuid.New(),
		runner:               NewConversationRunner(p.PoolMgr, p.MsgGen, p.Collector),
		msgGen:               p.MsgGen,
		logger:               slog.Default().With("vu", p.ID),
		collector:            p.Collector,
		conversationsEnabled: p.ConversationsEnabled,
	}
}

// Run executes the virtual user lifecycle until context is cancelled.
// Each iteration: create or continue conversation -> send message -> read history -> write response -> update metadata.
// The parent ctx signals "stop starting new work" (phase ending / Ctrl-C).
// Each MongoDB operation gets its own timeout so it never inherits a nearly-expired phase deadline.
func (vu *VirtualUser) Run(ctx context.Context) {
	var state *ConversationState

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if state == nil || !vu.msgGen.ShouldContinueConversation() {
			if vu.conversationsEnabled {
				opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
				newState, err := vu.runner.CreateConversation(opCtx, vu.userID)
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					vu.logger.Error("failed to create conversation", "err", err)
					vu.backoff(ctx)
					continue
				}
				state = newState
			} else {
				state = &ConversationState{
					ConversationID: uuid.New(),
					Model:          vu.msgGen.PickModel(),
					UserID:         vu.userID,
				}
			}
		}

		opCtx, cancel := context.WithTimeout(context.Background(), opTimeout)
		err := vu.runner.SendHumanMessage(opCtx, state)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			vu.logger.Error("failed to send human message", "err", err)
			state = nil
			vu.backoff(ctx)
			continue
		}

		opCtx, cancel = context.WithTimeout(context.Background(), opTimeout)
		_, err = vu.runner.ReadConversationHistory(opCtx, state)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			vu.logger.Error("failed to read conversation history", "err", err)
		}

		opCtx, cancel = context.WithTimeout(context.Background(), opTimeout)
		err = vu.runner.WriteAssistantResponse(opCtx, state)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			vu.logger.Error("failed to write assistant response", "err", err)
			state = nil
			vu.backoff(ctx)
			continue
		}

		if vu.conversationsEnabled {
			opCtx, cancel = context.WithTimeout(context.Background(), opTimeout)
			err = vu.runner.UpdateConversationMetadata(opCtx, state)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				vu.logger.Error("failed to update conversation metadata", "err", err)
			}
		}

		vu.simulateThinkTime(ctx)
	}
}

// simulateThinkTime adds a realistic delay between conversation turns,
// simulating LLM processing time and user reading time.
func (vu *VirtualUser) simulateThinkTime(ctx context.Context) {
	delay := 50*time.Millisecond + time.Duration(vu.msgGen.rng.IntN(200))*time.Millisecond
	select {
	case <-ctx.Done():
	case <-time.After(delay):
	}
}

func (vu *VirtualUser) backoff(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(500 * time.Millisecond):
	}
}
