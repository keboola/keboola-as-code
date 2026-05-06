package kaiagentsink

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SinkLoader loads a sink definition by its key.
type SinkLoader func(ctx context.Context, k key.SinkKey) (definition.Sink, error)

// Pipeline implements pipeline.Pipeline for the kaiAgent sink type.
// Each WriteRecord call POSTs to the kai-agent service.
type Pipeline struct {
	logger      log.Logger
	sinkKey     key.SinkKey
	sink        *definition.KaiAgentSink
	bridge      *Bridge
	baseURL     string
	token       string
	jsonnetPool *jsonnetWrapper.VMPool[recordctx.Context]
	onClose     func(ctx context.Context, cause string)

	sent             atomic.Uint64
	failed           atomic.Uint64
	firstSentAt      atomic.Pointer[utctime.UTCTime]
	lastSentAt       atomic.Pointer[utctime.UTCTime]
}

// ReopenOnSinkModification returns true so the pipeline is recreated when the sink definition changes.
func (p *Pipeline) ReopenOnSinkModification() bool {
	return true
}

// WriteRecord POSTs the incoming record to the configured kai-agent endpoint.
func (p *Pipeline) WriteRecord(c recordctx.Context) (pipeline.WriteResult, error) {
	switch p.sink.Mode {
	case definition.KaiAgentModeChat:
		return p.writeChat(c)
	case definition.KaiAgentModeSuggestions:
		return p.writeSuggestions(c)
	default:
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("kai-agent sink: unknown mode %q", p.sink.Mode)
	}
}

// --- chat mode ---

// chatMessagePart matches the "parts" element expected by POST /api/chat.
type chatMessagePart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// chatMessage matches the "message" field in the POST /api/chat body.
type chatMessage struct {
	ID    string            `json:"id"`
	Role  string            `json:"role"`
	Parts []chatMessagePart `json:"parts"`
}

// chatRequest is the POST /api/chat body.
type chatRequest struct {
	ID       string      `json:"id"`
	Message  chatMessage `json:"message"`
	BranchID int         `json:"branchId,omitempty"`
}

func (p *Pipeline) writeChat(c recordctx.Context) (pipeline.WriteResult, error) {
	// Resolve message text from template or raw body.
	text, err := p.resolveMessageText(c)
	if err != nil {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError}, err
	}

	// Use fixed chat ID from config or generate a new UUID per record.
	chatID := p.sink.ChatID
	if chatID == "" {
		chatID = newUUID()
	}

	body := chatRequest{
		ID: chatID,
		Message: chatMessage{
			ID:    newUUID(),
			Role:  "user",
			Parts: []chatMessagePart{{Type: "text", Text: text}},
		},
		BranchID: p.sink.BranchID,
	}

	url := p.baseURL + "/api/chat"
	statusCode, err := p.postJSON(c.Ctx(), url, body)
	if err != nil {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("kai-agent chat POST failed: %w", err)
	}
	if statusCode != http.StatusOK {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("kai-agent chat returned HTTP %d", statusCode)
	}

	p.recordSuccess(c)
	p.logger.Debugf(c.Ctx(), "kai-agent chat message sent for sink %s (chatId=%s)", p.sinkKey, chatID)
	return pipeline.WriteResult{Status: pipeline.RecordProcessed}, nil
}

func (p *Pipeline) resolveMessageText(c recordctx.Context) (string, error) {
	if p.sink.MessageTemplate == "" {
		// Use the raw body string as the message text.
		bodyBytes, err := c.BodyBytes()
		if err != nil {
			return "", errors.Errorf("kai-agent: cannot read request body: %w", err)
		}
		return string(bodyBytes), nil
	}

	vm := p.jsonnetPool.Get()
	result, err := jsonnet.Evaluate(vm, c, p.sink.MessageTemplate)
	p.jsonnetPool.Put(vm)
	if err != nil {
		return "", errors.Errorf("kai-agent messageTemplate evaluation failed: %w", err)
	}
	// Jsonnet wraps string output in quotes; strip them for plain text messages.
	result = strings.TrimSpace(result)
	if len(result) >= 2 && result[0] == '"' && result[len(result)-1] == '"' {
		var s string
		if jerr := json.Unmarshal([]byte(result), &s); jerr == nil {
			return s, nil
		}
	}
	return result, nil
}

// --- suggestions mode ---

// suggestionsRequest is the POST /api/suggestions body.
type suggestionsRequest struct {
	Context string         `json:"context"`
	Data    map[string]any `json:"data"`
}

// suggestionsResponse is the successful JSON response from POST /api/suggestions.
type suggestionsResponse struct {
	Suggestions        []suggestionItem `json:"suggestions"`
	SuggestionSessionID string          `json:"suggestionSessionId"`
}

type suggestionItem struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Prompt   string `json:"prompt"`
	Priority int    `json:"priority"`
	Category string `json:"category"`
	Reasoning string `json:"reasoning"`
}

func (p *Pipeline) writeSuggestions(c recordctx.Context) (pipeline.WriteResult, error) {
	data, err := p.resolveDataMap(c)
	if err != nil {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError}, err
	}

	body := suggestionsRequest{
		Context: string(p.sink.SuggestionsContext),
		Data:    data,
	}

	var resp suggestionsResponse
	url := p.baseURL + "/api/suggestions"
	statusCode, err := p.postJSONWithResult(c.Ctx(), url, body, &resp)
	if err != nil {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("kai-agent suggestions POST failed: %w", err)
	}
	if statusCode != http.StatusOK {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("kai-agent suggestions returned HTTP %d", statusCode)
	}

	p.recordSuccess(c)
	p.logger.Debugf(c.Ctx(), "kai-agent suggestions received %d items for sink %s (sessionId=%s)",
		len(resp.Suggestions), p.sinkKey, resp.SuggestionSessionID)
	return pipeline.WriteResult{Status: pipeline.RecordProcessed}, nil
}

func (p *Pipeline) resolveDataMap(c recordctx.Context) (map[string]any, error) {
	if p.sink.DataTemplate == "" {
		// Parse the raw JSON body and use it as the data map.
		bodyBytes, err := c.BodyBytes()
		if err != nil {
			return nil, errors.Errorf("kai-agent: cannot read request body: %w", err)
		}
		var data map[string]any
		if err := json.Unmarshal(bodyBytes, &data); err != nil {
			return nil, errors.Errorf("kai-agent: cannot parse request body as JSON object: %w", err)
		}
		return data, nil
	}

	vm := p.jsonnetPool.Get()
	result, err := jsonnet.Evaluate(vm, c, p.sink.DataTemplate)
	p.jsonnetPool.Put(vm)
	if err != nil {
		return nil, errors.Errorf("kai-agent dataTemplate evaluation failed: %w", err)
	}

	var data map[string]any
	if jerr := json.Unmarshal([]byte(strings.TrimSpace(result)), &data); jerr != nil {
		return nil, errors.Errorf("kai-agent dataTemplate must produce a JSON object, got %q: %w", result, jerr)
	}
	return data, nil
}

// --- HTTP helpers ---

// postJSON sends a JSON POST, discards the response body, and returns the HTTP status code.
// This is used for the chat endpoint whose response is an SSE stream (fire-and-forget).
func (p *Pipeline) postJSON(ctx context.Context, url string, body any) (int, error) {
	sender := p.bridge.HTTPClient()
	rawResp, _, err := sender.Send(ctx, request.NewHTTPRequest(sender).
		WithPost(url).
		WithJSONBody(body).
		AndHeader("x-storageapi-token", p.token))
	if rawResp != nil {
		// Discard the SSE body; the server continues processing asynchronously.
		_ = rawResp.Body.Close()
		return rawResp.StatusCode, err
	}
	return 0, err
}

// postJSONWithResult sends a JSON POST, reads and unmarshals the JSON response into result.
func (p *Pipeline) postJSONWithResult(ctx context.Context, url string, body, result any) (int, error) {
	sender := p.bridge.HTTPClient()
	rawResp, _, err := sender.Send(ctx, request.NewHTTPRequest(sender).
		WithPost(url).
		WithJSONBody(body).
		AndHeader("x-storageapi-token", p.token).
		WithResult(result))
	if rawResp != nil {
		return rawResp.StatusCode, err
	}
	return 0, err
}

// recordSuccess updates in-memory counters.
func (p *Pipeline) recordSuccess(c recordctx.Context) {
	now := utctime.From(c.Timestamp())
	p.sent.Add(1)
	p.firstSentAt.CompareAndSwap(nil, &now)
	p.lastSentAt.Store(&now)
}

// Close flushes accumulated stats to etcd and invokes the onClose callback.
func (p *Pipeline) Close(ctx context.Context, cause string) {
	sent := p.sent.Load()
	failed := p.failed.Load()

	if sent > 0 || failed > 0 {
		firstPtr := p.firstSentAt.Load()
		lastPtr := p.lastSentAt.Load()
		var firstAt, lastAt utctime.UTCTime
		if firstPtr != nil {
			firstAt = *firstPtr
		}
		if lastPtr != nil {
			lastAt = *lastPtr
		}
		if err := p.bridge.AddStats(ctx, p.sinkKey, sent, failed, firstAt, lastAt); err != nil {
			p.logger.Errorf(ctx, "failed to flush kai-agent stats for sink %s: %s", p.sinkKey, err)
		}
	}

	p.onClose(ctx, cause)
}

// NewOpener returns a pipeline.Opener for the SinkTypeKaiAgent sink type.
func NewOpener(logger log.Logger, bridge *Bridge, sinkLoader SinkLoader) pipeline.Opener {
	return func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
		if sinkType != definition.SinkTypeKaiAgent {
			return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
		}

		sink, err := sinkLoader(ctx, sinkKey)
		if err != nil {
			return nil, errors.Errorf("cannot load kai-agent sink definition for %s: %w", sinkKey, err)
		}
		if sink.KaiAgent == nil {
			return nil, errors.Errorf("sink %s has type %q but KaiAgent config is nil", sinkKey, sinkType)
		}

		token, err := bridge.TokenForSink(ctx, sinkKey)
		if err != nil {
			return nil, err
		}

		return &Pipeline{
			logger:      logger,
			sinkKey:     sinkKey,
			sink:        sink.KaiAgent,
			bridge:      bridge,
			baseURL:     bridge.BaseURL(),
			token:       token,
			jsonnetPool: jsonnet.NewPool(),
			onClose:     onClose,
		}, nil
	}
}

// newUUID generates a random UUID v4.
func newUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
