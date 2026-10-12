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

// Package ownedloop is the part of a recovery:full model harness that does not
// depend on the model's wire format (docs/conductor/ARCHITECTURE.md §1.4): the
// execution that picks a tool turn or a model turn from the queued history,
// the tool turn itself, which POSTs each unanswered call's arguments to that
// tool's own HTTP endpoint with the call_id as its Idempotency-Key, and the
// conductor_model_* metrics.
package ownedloop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	// MaxBody bounds how much of a response body is read.
	MaxBody = 10 << 20
	// malformedArgsKey holds a model's tool arguments that are not a JSON
	// object, so the tool turn can answer with an error instead of calling
	// the tool, and the next model turn sees what it sent.
	malformedArgsKey = "_conductor_malformed_arguments"
)

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

// The conductor_model_* metrics (docs/conductor/ARCHITECTURE.md §7.2).
// provider is the model endpoint's host.
var (
	ModelCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_model_calls_total",
		Help: "Model requests sent, whatever their outcome.",
	}, []string{"provider", "model"})
	ModelDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conductor_model_duration_seconds",
		Help:    "Model request latency, until the response is read or the request fails.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 40, 80, 160},
	}, []string{"provider", "model"})
	ModelTokens = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_model_tokens_total",
		Help: "Tokens the model endpoint reported in its usage block. Not recorded when the endpoint returns no usage.",
	}, []string{"provider", "model", "kind"})
)

// ObserveModelCall records one model request that started at start.
func ObserveModelCall(provider, model string, start time.Time) {
	ModelCalls.WithLabelValues(provider, model).Inc()
	ModelDuration.WithLabelValues(provider, model).Observe(time.Since(start).Seconds())
}

// Loop runs the owned-loop turn protocol for one harness.
type Loop struct {
	// Name prefixes errors.
	Name     string
	Client   *http.Client // for tool requests
	Tools    map[string]Tool
	MaxTurns int
	// ModelTurn performs one model turn over history and returns the steps
	// the model produced, in order.
	ModelTurn func(ctx context.Context, history []*proto.Step) ([]*proto.Step, error)
}

// Start returns a new execution of the loop.
func (l *Loop) Start() harness.Execution {
	return &execution{l: l, id: uuid.NewString()}
}

type execution struct {
	l  *Loop
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
		return fmt.Errorf("%s: Run needs the conversation history queued", e.l.Name)
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

func (e *execution) modelTurn(ctx context.Context, handler harness.Handler, history []*proto.Step) error {
	if n := modelTurns(history); n >= e.l.MaxTurns {
		return fmt.Errorf("%s: max_turns (%d) reached since the last user input", e.l.Name, e.l.MaxTurns)
	}
	steps, err := e.l.ModelTurn(ctx, history)
	if err != nil {
		return err
	}
	for _, s := range steps {
		if err := handler.OnMessage(ctx, e.id, s); err != nil {
			return err
		}
	}
	return nil
}

func (e *execution) toolTurn(ctx context.Context, handler harness.Handler, calls []*proto.ToolCallStep) error {
	for _, call := range calls {
		name := call.GetFunctionCall().GetName()
		result, isErr, err := e.l.callTool(ctx, call.GetId(), name, call.GetFunctionCall().GetArguments())
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
func (l *Loop) callTool(ctx context.Context, callID, name string, args *structpb.Struct) (result *proto.Value, isErr bool, err error) {
	tool, ok := l.Tools[name]
	if !ok {
		return errorResult(fmt.Sprintf("unknown tool %q", name), nil), true, nil
	}
	if raw, ok := malformed(args); ok {
		return errorResult("arguments are not a JSON object", map[string]any{"arguments": raw}), true, nil
	}
	body, err := ArgumentsJSON(args)
	if err != nil {
		return nil, false, fmt.Errorf("%s: tool %q args: %w", l.Name, name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tool.URL, strings.NewReader(body))
	if err != nil {
		return errorResult(err.Error(), nil), true, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", callID)
	resp, err := l.Client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return errorResult("tool request failed: "+err.Error(), nil), true, nil
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody))
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

// ParseArguments parses a model's tool arguments. Arguments that are not a
// JSON object are kept verbatim under malformedArgsKey.
func ParseArguments(raw string) *structpb.Struct {
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

// ArgumentsJSON is the inverse of ParseArguments. It uses encoding/json
// because protojson's output is deliberately unstable.
func ArgumentsJSON(args *structpb.Struct) (string, error) {
	if raw, ok := malformed(args); ok {
		return raw, nil
	}
	if args == nil {
		return "{}", nil
	}
	b, err := json.Marshal(args.AsMap())
	return string(b), err
}

// ResultText renders a tool result as message content: a string as is,
// anything else as JSON.
func ResultText(v *proto.Value) (string, error) {
	x := fromValue(v)
	if s, ok := x.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(x)
	if err != nil {
		return "", fmt.Errorf("encoding tool result: %w", err)
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
		return TextOf([]*proto.Content{k.ContentValue})
	}
	return nil
}

// TextOf concatenates the text of contents.
func TextOf(contents []*proto.Content) string {
	var b strings.Builder
	for _, c := range contents {
		b.WriteString(c.GetText().GetText())
	}
	return b.String()
}

// TextContent is a text Content.
func TextContent(text string) *proto.Content {
	return &proto.Content{Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}}}
}

// Truncate shortens s to n bytes for an error message.
func Truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
