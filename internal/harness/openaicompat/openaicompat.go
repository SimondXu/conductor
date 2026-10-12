// Copyright 2026 Simon Xu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package openaicompat is a recovery:full harness for any endpoint that speaks
// the OpenAI chat-completions wire format (docs/conductor/PRD.md §3). It
// follows the owned-loop turn protocol (docs/conductor/ARCHITECTURE.md §1.4):
// a model turn is one POST to {base_url}/chat/completions, and a tool turn
// POSTs each unanswered call's arguments to that tool's own HTTP endpoint with
// the call_id as its Idempotency-Key.
package openaicompat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

const (
	// DefaultRequestTimeout bounds each model and tool request.
	DefaultRequestTimeout = 120 * time.Second
	// DefaultMaxTurns caps the model turns since the last user input.
	DefaultMaxTurns = 24

	// maxBody bounds how much of a response body is read.
	maxBody = 10 << 20
	// malformedArgsKey holds a model's tool arguments that are not a JSON
	// object, so the tool turn can answer with an error instead of calling
	// the tool, and the next model turn sees what it sent.
	malformedArgsKey = "_conductor_malformed_arguments"
)

// Config is the registry.openai-compat configuration (docs/conductor/PRD.md
// §3.2).
type Config struct {
	BaseURL string `yaml:"base_url"`
	Model   string `yaml:"model"`
	// APIKeyEnv names the environment variable holding the key. The key
	// itself never appears in config or logs.
	APIKeyEnv      string        `yaml:"api_key_env,omitempty"`
	RequestTimeout time.Duration `yaml:"request_timeout,omitempty"`
	MaxTurns       int           `yaml:"max_turns,omitempty"`
	ToolChoice     string        `yaml:"tool_choice,omitempty"`
	// ExtraHeaders maps a header name to the environment variable holding
	// its value. Sent to the model endpoint only.
	ExtraHeaders map[string]string `yaml:"extra_headers,omitempty"`
	Tools        []Tool            `yaml:"tools,omitempty"`
}

// Tool is a tool the model may call. Its implementation is an HTTP endpoint.
type Tool struct {
	Name string `yaml:"name"`
	// Class is the recovery class. The controller's registry reads it; the
	// harness does not.
	Class       string         `yaml:"class"`
	Description string         `yaml:"description,omitempty"`
	Parameters  map[string]any `yaml:"parameters,omitempty"` // JSON schema
	URL         string         `yaml:"url"`
}

var modelTokens = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "conductor_model_tokens_total",
	Help: "Tokens the model endpoint reported in its usage block. Not recorded when the endpoint returns no usage.",
}, []string{"provider", "model", "kind"})

// Harness is the OpenAI-compatible harness.
type Harness struct {
	cfg      Config
	client   *http.Client
	tools    map[string]Tool
	provider string // base_url host, the provider label of token metrics
}

var _ harness.Harness = (*Harness)(nil)

// New returns a harness for cfg, applying defaults.
func New(cfg Config) (*Harness, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("openaicompat: base_url %q is not an absolute URL", cfg.BaseURL)
	}
	if cfg.Model == "" {
		return nil, errors.New("openaicompat: model is required")
	}
	cfg.BaseURL = strings.TrimSuffix(cfg.BaseURL, "/")
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = DefaultMaxTurns
	}
	if cfg.ToolChoice == "" {
		cfg.ToolChoice = "auto"
	}
	tools := map[string]Tool{}
	for _, t := range cfg.Tools {
		tools[t.Name] = t
	}
	return &Harness{
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.RequestTimeout},
		tools:    tools,
		provider: u.Host,
	}, nil
}

// Start implements harness.Harness. agent_config is not used.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return &execution{h: h, id: uuid.NewString()}, nil
}

type execution struct {
	h  *Harness
	id string

	mu     sync.Mutex
	queued []*proto.Step
}

func (e *execution) ID() string { return e.id }

// Queue implements harness.Execution. Under the owned-loop protocol the
// queue holds the whole history; Run consumes it.
func (e *execution) Queue(ctx context.Context, steps ...*proto.Step) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queued = append(e.queued, steps...)
	return nil
}

func (e *execution) Close(ctx context.Context) error { return nil }

// Run performs one turn: a tool turn if the history ends in unanswered tool
// calls, a model turn otherwise.
func (e *execution) Run(ctx context.Context, handler harness.Handler) error {
	e.mu.Lock()
	history := e.queued
	e.queued = nil
	e.mu.Unlock()
	if len(history) == 0 {
		return errors.New("openaicompat: Run needs the conversation history queued")
	}

	var err error
	if calls := harness.UnansweredToolCalls(history); len(calls) > 0 {
		err = e.toolTurn(ctx, handler, calls)
	} else {
		err = e.modelTurn(ctx, handler, history)
	}
	if err != nil {
		return err
	}
	return handler.OnComplete(ctx, e.id)
}

// Chat-completions wire types, limited to the fields this harness uses.
type (
	chatRequest struct {
		Model      string    `json:"model"`
		Messages   []message `json:"messages"`
		Tools      []toolDef `json:"tools,omitempty"`
		ToolChoice string    `json:"tool_choice,omitempty"`
	}
	message struct {
		Role       string     `json:"role"`
		Content    *string    `json:"content"`
		ToolCalls  []toolCall `json:"tool_calls,omitempty"`
		ToolCallID string     `json:"tool_call_id,omitempty"`
	}
	toolCall struct {
		ID       string       `json:"id"`
		Type     string       `json:"type"`
		Function functionCall `json:"function"`
	}
	functionCall struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	toolDef struct {
		Type     string      `json:"type"`
		Function functionDef `json:"function"`
	}
	functionDef struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters,omitempty"`
	}
	chatResponse struct {
		Choices []struct {
			Message      message `json:"message"`
			FinishReason string  `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
)

func (e *execution) modelTurn(ctx context.Context, handler harness.Handler, history []*proto.Step) error {
	h := e.h
	if n := modelTurns(history); n >= h.cfg.MaxTurns {
		return fmt.Errorf("openaicompat: max_turns (%d) reached since the last user input", h.cfg.MaxTurns)
	}
	msgs, err := messages(history)
	if err != nil {
		return err
	}
	req := chatRequest{Model: h.cfg.Model, Messages: msgs}
	for _, t := range h.cfg.Tools {
		req.Tools = append(req.Tools, toolDef{Type: "function", Function: functionDef{
			Name: t.Name, Description: t.Description, Parameters: t.Parameters,
		}})
	}
	if len(req.Tools) > 0 {
		req.ToolChoice = h.cfg.ToolChoice
	}
	resp, err := h.complete(ctx, req)
	if err != nil {
		return err
	}
	if resp.Usage != nil {
		modelTokens.WithLabelValues(h.provider, h.cfg.Model, "prompt").Add(float64(resp.Usage.PromptTokens))
		modelTokens.WithLabelValues(h.provider, h.cfg.Model, "completion").Add(float64(resp.Usage.CompletionTokens))
	}
	if len(resp.Choices) == 0 {
		return errors.New("openaicompat: response has no choices")
	}
	choice := resp.Choices[0]
	msg := choice.Message
	content := ""
	if msg.Content != nil {
		content = *msg.Content
	}
	if content == "" && len(msg.ToolCalls) == 0 {
		return fmt.Errorf("openaicompat: model returned neither content nor tool calls (finish_reason %q)", choice.FinishReason)
	}

	var steps []*proto.Step
	if content != "" {
		steps = append(steps, &proto.Step{Type: &proto.Step_Content{Content: &proto.ContentStep{
			Role: "assistant", Content: []*proto.Content{textContent(content)},
		}}})
	}
	for _, tc := range msg.ToolCalls {
		steps = append(steps, &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
			// The controller replaces this with Conductor's call_id.
			Id: tc.ID,
			Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{
				Name: tc.Function.Name, Arguments: parseArguments(tc.Function.Arguments),
			}},
		}}})
	}
	for _, s := range steps {
		if err := handler.OnMessage(ctx, e.id, s); err != nil {
			return err
		}
	}
	return nil
}

// complete POSTs req to the chat-completions endpoint.
func (h *Harness) complete(ctx context.Context, req chatRequest) (*chatResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("openaicompat: encoding request: %w", err)
	}
	endpoint := h.cfg.BaseURL + "/chat/completions"
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openaicompat: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if h.cfg.APIKeyEnv != "" {
		if key := os.Getenv(h.cfg.APIKeyEnv); key != "" {
			hreq.Header.Set("Authorization", "Bearer "+key)
		}
	}
	for name, env := range h.cfg.ExtraHeaders {
		if v := os.Getenv(env); v != "" {
			hreq.Header.Set(name, v)
		}
	}
	resp, err := h.client.Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("openaicompat: model request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("openaicompat: reading model response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("openaicompat: %s returned %s: %s", endpoint, resp.Status, truncate(string(data), 512))
	}
	var out chatResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("openaicompat: decoding model response: %w", err)
	}
	return &out, nil
}

func (e *execution) toolTurn(ctx context.Context, handler harness.Handler, calls []*proto.ToolCallStep) error {
	for _, call := range calls {
		name := call.GetFunctionCall().GetName()
		result, isErr, err := e.h.callTool(ctx, call.GetId(), name, call.GetFunctionCall().GetArguments())
		if err != nil {
			return err
		}
		step := &proto.Step{Type: &proto.Step_ToolResult{ToolResult: &proto.ToolResultStep{
			CallId: call.GetId(),
			Type: &proto.ToolResultStep_FunctionResult{FunctionResult: &proto.FunctionResultStep{
				Name: name, IsError: isErr, Result: result,
			}},
		}}}
		if err := handler.OnMessage(ctx, e.id, step); err != nil {
			return err
		}
	}
	return nil
}

// callTool POSTs a call's arguments to its tool's endpoint. A failed call is a
// result with isErr set, which the model sees; err is returned only when ctx
// ends, so the controller does not settle a call it cancelled.
func (h *Harness) callTool(ctx context.Context, callID, name string, args *structpb.Struct) (result *proto.Value, isErr bool, err error) {
	tool, ok := h.tools[name]
	if !ok {
		return errorResult(fmt.Sprintf("unknown tool %q", name), nil), true, nil
	}
	if raw, ok := malformed(args); ok {
		return errorResult("arguments are not a JSON object", map[string]any{"arguments": raw}), true, nil
	}
	body, err := argumentsJSON(args)
	if err != nil {
		return nil, false, fmt.Errorf("openaicompat: tool %q args: %w", name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tool.URL, strings.NewReader(body))
	if err != nil {
		return errorResult(err.Error(), nil), true, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", callID)
	resp, err := h.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return errorResult("tool request failed: "+err.Error(), nil), true, nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return errorResult("reading tool response: "+err.Error(), nil), true, nil
	}
	if resp.StatusCode/100 != 2 {
		return errorResult("tool endpoint returned "+resp.Status, map[string]any{
			"status": resp.StatusCode, "body": jsonOrString(data),
		}), true, nil
	}
	return toValue(jsonOrString(data)), false, nil
}

// messages converts the history to chat messages. Thoughts are not sent back.
func messages(history []*proto.Step) ([]message, error) {
	var msgs []message
	for _, s := range history {
		switch {
		case s.GetContent() != nil:
			role := s.GetContent().GetRole()
			if role == "" {
				role = "user"
			}
			text := textOf(s.GetContent().GetContent())
			msgs = append(msgs, message{Role: role, Content: &text})
		case s.GetToolCall() != nil:
			fc := s.GetToolCall().GetFunctionCall()
			args, err := argumentsJSON(fc.GetArguments())
			if err != nil {
				return nil, fmt.Errorf("openaicompat: tool %q args: %w", fc.GetName(), err)
			}
			tc := toolCall{ID: s.GetToolCall().GetId(), Type: "function", Function: functionCall{Name: fc.GetName(), Arguments: args}}
			// Calls from one model turn belong to one assistant message.
			if n := len(msgs); n > 0 && msgs[n-1].Role == "assistant" {
				msgs[n-1].ToolCalls = append(msgs[n-1].ToolCalls, tc)
			} else {
				msgs = append(msgs, message{Role: "assistant", ToolCalls: []toolCall{tc}})
			}
		case s.GetToolResult() != nil:
			content, err := resultText(s.GetToolResult().GetFunctionResult().GetResult())
			if err != nil {
				return nil, err
			}
			msgs = append(msgs, message{Role: "tool", ToolCallID: s.GetToolResult().GetCallId(), Content: &content})
		}
	}
	return msgs, nil
}

// modelTurns counts the model turns since the last user input: the runs of
// consecutive model output.
func modelTurns(history []*proto.Step) int {
	n, prevOutput := 0, false
	for _, s := range history {
		if s.GetContent().GetRole() == "user" {
			n, prevOutput = 0, false
			continue
		}
		output := s.GetContent() != nil || s.GetThought() != nil || s.GetToolCall() != nil
		if output && !prevOutput {
			n++
		}
		prevOutput = output
	}
	return n
}

// parseArguments parses a model's tool arguments. Arguments that are not a
// JSON object are kept verbatim under malformedArgsKey.
func parseArguments(raw string) *structpb.Struct {
	args := &structpb.Struct{}
	if strings.TrimSpace(raw) == "" {
		return args
	}
	if err := protojson.Unmarshal([]byte(raw), args); err != nil {
		return &structpb.Struct{Fields: map[string]*structpb.Value{malformedArgsKey: structpb.NewStringValue(raw)}}
	}
	return args
}

func malformed(args *structpb.Struct) (string, bool) {
	v, ok := args.GetFields()[malformedArgsKey]
	if !ok || len(args.GetFields()) != 1 {
		return "", false
	}
	return v.GetStringValue(), true
}

// argumentsJSON is the inverse of parseArguments. It uses encoding/json
// because protojson's output is deliberately unstable.
func argumentsJSON(args *structpb.Struct) (string, error) {
	if raw, ok := malformed(args); ok {
		return raw, nil
	}
	if args == nil {
		return "{}", nil
	}
	b, err := json.Marshal(args.AsMap())
	return string(b), err
}

// resultText renders a tool result as tool message content: a string as is,
// anything else as JSON.
func resultText(v *proto.Value) (string, error) {
	x := fromValue(v)
	if s, ok := x.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(x)
	if err != nil {
		return "", fmt.Errorf("openaicompat: encoding tool result: %w", err)
	}
	return string(b), nil
}

func errorResult(msg string, extra map[string]any) *proto.Value {
	m := map[string]any{"error": msg}
	for k, v := range extra {
		m[k] = v
	}
	return toValue(m)
}

// jsonOrString decodes data as JSON, or returns it as a string if it is not.
func jsonOrString(data []byte) any {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return string(data)
	}
	return v
}

// toValue converts a decoded JSON value (or a map of such values) to a Value.
func toValue(x any) *proto.Value {
	switch x := x.(type) {
	case nil:
		return &proto.Value{Kind: &proto.Value_NullValue{}}
	case bool:
		return &proto.Value{Kind: &proto.Value_BoolValue{BoolValue: x}}
	case float64:
		return &proto.Value{Kind: &proto.Value_NumberValue{NumberValue: x}}
	case string:
		return &proto.Value{Kind: &proto.Value_StringValue{StringValue: x}}
	case []any:
		l := &proto.ListValue{}
		for _, e := range x {
			l.Values = append(l.Values, toValue(e))
		}
		return &proto.Value{Kind: &proto.Value_ListValue{ListValue: l}}
	case map[string]any:
		s, err := structpb.NewStruct(x)
		if err != nil {
			return &proto.Value{Kind: &proto.Value_StringValue{StringValue: fmt.Sprint(x)}}
		}
		return &proto.Value{Kind: &proto.Value_StructValue{StructValue: s}}
	}
	return &proto.Value{Kind: &proto.Value_StringValue{StringValue: fmt.Sprint(x)}}
}

func fromValue(v *proto.Value) any {
	switch k := v.GetKind().(type) {
	case *proto.Value_BoolValue:
		return k.BoolValue
	case *proto.Value_NumberValue:
		return k.NumberValue
	case *proto.Value_StringValue:
		return k.StringValue
	case *proto.Value_StructValue:
		return k.StructValue.AsMap()
	case *proto.Value_ListValue:
		out := []any{}
		for _, e := range k.ListValue.GetValues() {
			out = append(out, fromValue(e))
		}
		return out
	case *proto.Value_ContentValue:
		return textOf([]*proto.Content{k.ContentValue})
	}
	return nil
}

func textOf(contents []*proto.Content) string {
	var b strings.Builder
	for _, c := range contents {
		b.WriteString(c.GetText().GetText())
	}
	return b.String()
}

func textContent(text string) *proto.Content {
	return &proto.Content{Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}}}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
