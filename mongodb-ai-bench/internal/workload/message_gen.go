package workload

import (
	"crypto/rand"
	"fmt"
	"math/big"
	mathrand "math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb-ai-bench/internal/config"
	"github.com/mongodb-ai-bench/internal/data"
)

type MessageGenerator struct {
	rng *mathrand.Rand
	cfg config.WorkloadConfig
}

type GeneratedMessage struct {
	ID               uuid.UUID
	ConversationID   uuid.UUID
	UserID           uuid.UUID
	Message          string
	Sender           string
	CreateTime       time.Time
	ParentResponseID *uuid.UUID
	Metadata         map[string]interface{}
	Model            string
	ToolResponses    []interface{}
	WebSearchResults []map[string]interface{}
}

func NewMessageGenerator(cfg config.WorkloadConfig, seed uint64) *MessageGenerator {
	src := mathrand.NewPCG(seed, seed^0xdeadbeef)
	return &MessageGenerator{
		rng: mathrand.New(src),
		cfg: cfg,
	}
}

func (mg *MessageGenerator) GenerateHumanMessage(conversationID uuid.UUID, userID uuid.UUID, parentID *uuid.UUID, model string) GeneratedMessage {
	var message string
	if mg.rng.IntN(100) < 15 {
		message = mg.generateLongPrompt()
	} else {
		message = data.HumanPrompts[mg.rng.IntN(len(data.HumanPrompts))]
	}

	return GeneratedMessage{
		ID:               uuid.New(),
		ConversationID:   conversationID,
		UserID:           userID,
		Message:          message,
		Sender:           "human",
		CreateTime:       time.Now(),
		ParentResponseID: parentID,
		Metadata: map[string]interface{}{
			"requestModelDetails": map[string]interface{}{
				"modelId": model,
			},
		},
		Model:         model,
		ToolResponses: []interface{}{},
	}
}

func (mg *MessageGenerator) GenerateAssistantMessage(conversationID uuid.UUID, userID uuid.UUID, parentID uuid.UUID, requestModel string) GeneratedMessage {
	responseModel := mg.cfg.Models[mg.rng.IntN(len(mg.cfg.Models))]

	roll := mg.rng.IntN(100)
	dist := mg.cfg.ResponseSizeDistribution
	var message string
	var webResults []map[string]interface{}

	switch {
	case roll < dist.ShortPct:
		message = mg.generateShortResponse()
	case roll < dist.ShortPct+dist.MediumPct:
		message = mg.generateMediumResponse()
	case roll < dist.ShortPct+dist.MediumPct+dist.LongPct:
		message = mg.generateLongResponse()
	default:
		message = mg.generateVeryLongResponse()
		if mg.rng.IntN(100) < mg.cfg.WebSearchPct*10 {
			webResults = mg.generateWebSearchResults()
		}
	}

	traceID := make([]byte, 16)
	rand.Read(traceID)

	metadata := map[string]interface{}{
		"deepsearchPreset": "",
		"ui_layout": map[string]interface{}{
			"reasoningUiLayout": "FUNCTION_CALL",
			"willThinkLong":     mg.rng.IntN(100) < 20,
			"effort":            mg.pickEffort(),
		},
		"llm_info": map[string]interface{}{
			"modelHash": mg.randomHash(),
		},
		"request_metadata": map[string]interface{}{
			"model":  requestModel,
			"mode":   "auto",
			"effort": strings.ToLower(mg.pickEffort()),
		},
		"request_trace_id": fmt.Sprintf("%x", traceID),
	}

	pid := parentID
	return GeneratedMessage{
		ID:               uuid.New(),
		ConversationID:   conversationID,
		UserID:           userID,
		Message:          message,
		Sender:           "assistant",
		CreateTime:       time.Now(),
		ParentResponseID: &pid,
		Metadata:         metadata,
		Model:            responseModel,
		ToolResponses:    []interface{}{},
		WebSearchResults: webResults,
	}
}

func (mg *MessageGenerator) generateShortResponse() string {
	prefix := data.ShortResponses[mg.rng.IntN(len(data.ShortResponses))]
	paragraph := data.TechnicalParagraphs[mg.rng.IntN(len(data.TechnicalParagraphs))]
	targetLen := 100 + mg.rng.IntN(400)
	result := prefix + paragraph
	if len(result) > targetLen {
		result = result[:targetLen]
	}
	return result
}

func (mg *MessageGenerator) generateMediumResponse() string {
	tmpl := data.MediumResponseTemplates[mg.rng.IntN(len(data.MediumResponseTemplates))]
	needed := countFormatVerbs(tmpl)
	paragraphs := mg.pickN(data.TechnicalParagraphs, needed)
	args := make([]interface{}, len(paragraphs))
	for i, p := range paragraphs {
		args[i] = p
	}
	result := fmt.Sprintf(tmpl, args...)
	targetLen := 500 + mg.rng.IntN(2500)
	if len(result) > targetLen {
		result = result[:targetLen]
	} else {
		for len(result) < targetLen {
			result += "\n\n" + data.TechnicalParagraphs[mg.rng.IntN(len(data.TechnicalParagraphs))]
		}
		result = result[:targetLen]
	}
	return result
}

func (mg *MessageGenerator) generateLongResponse() string {
	tmpl := data.LongResponseTemplates[mg.rng.IntN(len(data.LongResponseTemplates))]
	fills := mg.pickN(data.TechnicalParagraphs, 15)
	mixed := make([]interface{}, 0, 20)
	for _, f := range fills {
		mixed = append(mixed, f)
	}
	for len(mixed) < 20 {
		mixed = append(mixed, data.PerformanceValues[mg.rng.IntN(len(data.PerformanceValues))])
	}
	result := fmt.Sprintf(tmpl, mixed[:countFormatVerbs(tmpl)]...)
	targetLen := 3000 + mg.rng.IntN(7000)
	for len(result) < targetLen {
		result += "\n\n" + data.TechnicalParagraphs[mg.rng.IntN(len(data.TechnicalParagraphs))]
	}
	if len(result) > targetLen {
		result = result[:targetLen]
	}
	return result
}

func (mg *MessageGenerator) generateVeryLongResponse() string {
	targetLen := 10000 + mg.rng.IntN(90000)
	result := mg.generateLongResponse()
	for len(result) < targetLen {
		result += "\n\n---\n\n" + data.TechnicalParagraphs[mg.rng.IntN(len(data.TechnicalParagraphs))]
	}
	if len(result) > targetLen {
		result = result[:targetLen]
	}
	return result
}

func (mg *MessageGenerator) generateWebSearchResults() []map[string]interface{} {
	count := 3 + mg.rng.IntN(8)
	results := make([]map[string]interface{}, 0, count)
	for i := 0; i < count; i++ {
		src := data.WebSearchResults[mg.rng.IntN(len(data.WebSearchResults))]
		results = append(results, map[string]interface{}{
			"url":                src["url"],
			"title":              src["title"],
			"preview":            src["preview"],
			"search_engine_text": "",
			"description":        "",
			"site_name":          "",
			"metadata_title":     "",
			"creator":            "",
			"image":              "",
			"favicon":            "",
		})
	}
	return results
}

func (mg *MessageGenerator) generateLongPrompt() string {
	tmpl := data.LongPromptTemplates[mg.rng.IntN(len(data.LongPromptTemplates))]
	fills := make([]interface{}, 0, 15)
	sources := [][]string{
		data.ApplicationTypes, data.ScaleDescriptors, data.TechStacks,
		data.BottleneckTypes, data.InfraDescriptors, data.MemoryDescriptors,
		data.DatabaseTypes, data.DataSizes, data.TechnicalParagraphs,
	}
	for i := 0; i < 15; i++ {
		src := sources[i%len(sources)]
		fills = append(fills, src[mg.rng.IntN(len(src))])
	}
	needed := countFormatVerbs(tmpl)
	if needed > len(fills) {
		for len(fills) < needed {
			fills = append(fills, data.TechnicalParagraphs[mg.rng.IntN(len(data.TechnicalParagraphs))])
		}
	}
	return fmt.Sprintf(tmpl, fills[:needed]...)
}

func (mg *MessageGenerator) pickEffort() string {
	efforts := []string{"LOW", "MEDIUM", "HIGH"}
	return efforts[mg.rng.IntN(len(efforts))]
}

func (mg *MessageGenerator) randomHash() string {
	b := make([]byte, 32)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func (mg *MessageGenerator) pickN(items []string, n int) []string {
	if n > len(items) {
		n = len(items)
	}
	picked := make([]string, n)
	for i := 0; i < n; i++ {
		picked[i] = items[mg.rng.IntN(len(items))]
	}
	return picked
}

func (mg *MessageGenerator) PickModel() string {
	return mg.cfg.Models[mg.rng.IntN(len(mg.cfg.Models))]
}

func (mg *MessageGenerator) ShouldContinueConversation() bool {
	return mg.rng.IntN(100) < mg.cfg.ContinueConversationPct
}

func (mg *MessageGenerator) RandomSeed() uint64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	return n.Uint64()
}

func countFormatVerbs(s string) int {
	count := 0
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '%' && s[i+1] == 's' {
			count++
		}
	}
	return count
}
