package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/mongodb-ai-bench/internal/metrics"
	"github.com/mongodb-ai-bench/internal/pool"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var uriPattern = regexp.MustCompile(`mongodb(\+srv)?://[^\s,]+`)

type ConversationState struct {
	ConversationID uuid.UUID
	LastMessageID  uuid.UUID
	Model          string
	MessageCount   int
	UserID         uuid.UUID
}

type ConversationRunner struct {
	poolMgr            *pool.Manager
	msgGen             *MessageGenerator
	collector          *metrics.Collector
	maxHistoryMessages int64
}

func NewConversationRunner(poolMgr *pool.Manager, msgGen *MessageGenerator, collector *metrics.Collector, maxHistoryMessages int) *ConversationRunner {
	return &ConversationRunner{
		poolMgr:            poolMgr,
		msgGen:             msgGen,
		collector:          collector,
		maxHistoryMessages: int64(maxHistoryMessages),
	}
}

func (cr *ConversationRunner) CreateConversation(ctx context.Context, userID uuid.UUID) (*ConversationState, error) {
	model := cr.msgGen.PickModel()
	convoID := uuid.New()
	now := time.Now()

	doc := bson.M{
		"_id":             convoID.String(),
		"user_id":         userID.String(),
		"model":           model,
		"created_at":      now,
		"last_message_at": now,
		"message_count":   0,
	}

	start := time.Now()
	_, err := cr.poolMgr.ConversationsCollection().InsertOne(ctx, doc)
	e2e := time.Since(start)

	cr.collector.Record(metrics.Sample{
		Operation:         metrics.OpCreateConversation,
		E2ELatency:        e2e,
		Success:           err == nil,
		Error:             errStr(err),
		Timestamp:         time.Now(),
		DocumentSizeBytes: estimateDocSize(doc),
	})

	if err != nil {
		return nil, fmt.Errorf("creating conversation: %w", err)
	}

	return &ConversationState{
		ConversationID: convoID,
		Model:          model,
		UserID:         userID,
	}, nil
}

func (cr *ConversationRunner) SendHumanMessage(ctx context.Context, state *ConversationState) error {
	var parentID *uuid.UUID
	if state.MessageCount > 0 {
		parentID = &state.LastMessageID
	}

	msg := cr.msgGen.GenerateHumanMessage(state.ConversationID, state.UserID, parentID, state.Model)
	doc := messageToDoc(msg)

	start := time.Now()
	_, err := cr.poolMgr.MessagesCollection().InsertOne(ctx, doc)
	e2e := time.Since(start)

	cr.collector.Record(metrics.Sample{
		Operation:         metrics.OpWriteHumanMessage,
		E2ELatency:        e2e,
		Success:           err == nil,
		Error:             errStr(err),
		Timestamp:         time.Now(),
		DocumentSizeBytes: estimateDocSize(doc),
	})

	if err != nil {
		return fmt.Errorf("writing human message: %w", err)
	}

	state.LastMessageID = msg.ID
	state.MessageCount++
	return nil
}

func (cr *ConversationRunner) ReadConversationHistory(ctx context.Context, state *ConversationState) (int, error) {
	filter := bson.M{"conversation_id": state.ConversationID.String()}
	findOpts := options.Find().
		SetSort(bson.D{{Key: "create_time", Value: 1}}).
		SetLimit(cr.maxHistoryMessages)

	start := time.Now()
	cursor, err := cr.poolMgr.MessagesCollection().Find(ctx, filter, findOpts)
	if err != nil {
		e2e := time.Since(start)
		cr.collector.Record(metrics.Sample{
			Operation:  metrics.OpReadConvoHistory,
			E2ELatency: e2e,
			Success:    false,
			Error:      errStr(err),
			Timestamp:  time.Now(),
		})
		return 0, fmt.Errorf("reading conversation history: %w", err)
	}

	var results []bson.M
	err = cursor.All(ctx, &results)
	e2e := time.Since(start)

	totalBytes := 0
	for _, r := range results {
		totalBytes += estimateDocSize(r)
	}

	cr.collector.Record(metrics.Sample{
		Operation:         metrics.OpReadConvoHistory,
		E2ELatency:        e2e,
		Success:           err == nil,
		Error:             errStr(err),
		Timestamp:         time.Now(),
		DocumentSizeBytes: totalBytes,
	})

	if err != nil {
		return 0, fmt.Errorf("decoding conversation history: %w", err)
	}
	return len(results), nil
}

func (cr *ConversationRunner) WriteAssistantResponse(ctx context.Context, state *ConversationState) error {
	msg := cr.msgGen.GenerateAssistantMessage(state.ConversationID, state.UserID, state.LastMessageID, state.Model)
	doc := messageToDoc(msg)

	start := time.Now()
	_, err := cr.poolMgr.MessagesCollection().InsertOne(ctx, doc)
	e2e := time.Since(start)

	cr.collector.Record(metrics.Sample{
		Operation:         metrics.OpWriteAssistantMsg,
		E2ELatency:        e2e,
		Success:           err == nil,
		Error:             errStr(err),
		Timestamp:         time.Now(),
		DocumentSizeBytes: estimateDocSize(doc),
	})

	if err != nil {
		return fmt.Errorf("writing assistant message: %w", err)
	}

	state.LastMessageID = msg.ID
	state.MessageCount++
	return nil
}

func (cr *ConversationRunner) UpdateConversationMetadata(ctx context.Context, state *ConversationState) error {
	filter := bson.M{"_id": state.ConversationID.String()}
	update := bson.M{
		"$set": bson.M{"last_message_at": time.Now()},
		"$inc": bson.M{"message_count": 2},
	}

	start := time.Now()
	_, err := cr.poolMgr.ConversationsCollection().UpdateOne(ctx, filter, update)
	e2e := time.Since(start)

	cr.collector.Record(metrics.Sample{
		Operation:  metrics.OpWriteConvoMetadata,
		E2ELatency: e2e,
		Success:    err == nil,
		Error:      errStr(err),
		Timestamp:  time.Now(),
	})

	if err != nil {
		return fmt.Errorf("updating conversation metadata: %w", err)
	}
	return nil
}

func messageToDoc(msg GeneratedMessage) bson.M {
	doc := bson.M{
		"_id":             msg.ID.String(),
		"conversation_id": msg.ConversationID.String(),
		"user_id":         msg.UserID.String(),
		"message":         msg.Message,
		"sender":          msg.Sender,
		"create_time":     msg.CreateTime,
		"metadata":        msg.Metadata,
		"model":           msg.Model,
		"tool_responses":  msg.ToolResponses,
	}
	if msg.ParentResponseID != nil {
		doc["parent_response_id"] = msg.ParentResponseID.String()
	}
	if len(msg.WebSearchResults) > 0 {
		doc["web_search_results"] = msg.WebSearchResults
	}
	return doc
}

func estimateDocSize(doc bson.M) int {
	data, err := json.Marshal(doc)
	if err != nil {
		return 256
	}
	return len(data)
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return uriPattern.ReplaceAllString(err.Error(), "[REDACTED_URI]")
}
