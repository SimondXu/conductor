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

package openaicompat

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// modelServer is a fake chat-completions endpoint that serves scripted
// responses in order and records each request.
type modelServer struct {
	t         *testing.T
	responses []string

	mu       sync.Mutex
	requests []chatRequest
	headers  []http.Header
}

func (m *modelServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/chat/completions" {
		m.t.Errorf("model request to %s", r.URL.Path)
	}
	var req chatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.t.Errorf("decoding model request: %v", err)
	}
	m.mu.Lock()
	n := len(m.requests)
	m.requests = append(m.requests, req)
	m.headers = append(m.headers, r.Header.Clone())
	m.mu.Unlock()
	if n >= len(m.responses) {
		m.t.Errorf("unexpected model request %d", n+1)
		http.Error(w, "script exhausted", http.StatusInternalServerError)
		return
	}
	io.WriteString(w, m.responses[n])
}

type toolRequest struct {
	path, key, body string
}

// toolServer serves /weather with a JSON result and /down with a 503.
type toolServer struct {
	mu       sync.Mutex
	requests []toolRequest
}

func (s *toolServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, toolRequest{r.URL.Path, r.Header.Get("Idempotency-Key"), string(body)})
	s.mu.Unlock()
	if r.URL.Path == "/down" {
		http.Error(w, "upstream down", http.StatusServiceUnavailable)
		return
	}
	var args map[string]any
	json.Unmarshal(body, &args)
	json.NewEncoder(w).Encode(map[string]any{"city": args["city"], "temp_c": 21})
}

type rig struct {
	model *modelServer
	tools *toolServer
	log   *eventlogtest.MemoryEventLog
	c     *controller.Controller
	cfg   Config
}

func newRig(t *testing.T, responses ...string) *rig {
	t.Helper()
	r := &rig{model: &modelServer{t: t, responses: responses}, tools: &toolServer{}, log: &eventlogtest.MemoryEventLog{}}
	ms := httptest.NewServer(r.model)
	ts := httptest.NewServer(r.tools)
	t.Cleanup(ms.Close)
	t.Cleanup(ts.Close)
	r.cfg = Config{
		BaseURL: ms.URL + "/v1",
		Model:   "test-model",
		Tools: []Tool{
			{Name: "get_weather", Class: "IDEMPOTENT", Description: "Weather for a city.", URL: ts.URL + "/weather",
				Parameters: map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}}},
			{Name: "page_oncall", Class: "UNSAFE", URL: ts.URL + "/down"},
		},
	}
	return r
}

// exec runs one Exec of the owned loop over a fresh controller.
func (r *rig) exec(t *testing.T, conv string) error {
	t.Helper()
	h, err := New(r.cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := controller.NewRegistry()
	if err := reg.Register("openai-compat", h, controller.RecoveryFull); err != nil {
		t.Fatal(err)
	}
	c, err := controller.New(context.Background(), controller.Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return r.log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: conv,
		AgentId:        "openai-compat",
		Inputs:         []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
}

// steps returns the logged steps and checks the conversation completed.
func (r *rig) steps(t *testing.T, conv string) []*proto.Step {
	t.Helper()
	events, err := r.log.Events(context.Background(), conv)
	if err != nil {
		t.Fatal(err)
	}
	if last := events[len(events)-1]; last.State != proto.State_STATE_COMPLETED {
		t.Fatalf("last event state = %v, want COMPLETED", last.State)
	}
	var out []*proto.Step
	for _, ev := range events {
		out = append(out, ev.Steps...)
	}
	return out
}

func toolCallsResponse(usage string, calls ...string) string {
	var tcs []string
	for i := 0; i < len(calls); i += 3 {
		args, _ := json.Marshal(calls[i+2])
		tcs = append(tcs, `{"id":"`+calls[i]+`","type":"function","function":{"name":"`+calls[i+1]+`","arguments":`+string(args)+`}}`)
	}
	return `{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[` + strings.Join(tcs, ",") + `]},"finish_reason":"tool_calls"}]` + usage + `}`
}

func contentResponse(text, usage string) string {
	return `{"choices":[{"message":{"role":"assistant","content":"` + text + `"},"finish_reason":"stop"}]` + usage + `}`
}

func tokens(t *testing.T, cfg Config, kind string) float64 {
	t.Helper()
	u, _ := url.Parse(cfg.BaseURL)
	m := &dto.Metric{}
	if err := modelTokens.WithLabelValues(u.Host, cfg.Model, kind).Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

var callIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestOwnedLoop_ParallelToolCalls(t *testing.T) {
	const usage = `,"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}`
	r := newRig(t,
		toolCallsResponse(usage, "prov_a", "get_weather", `{"city":"Paris"}`, "prov_b", "get_weather", `{"city":"Oslo"}`),
		contentResponse("Both 21C.", usage),
	)
	t.Setenv("TEST_OPENAI_KEY", "sk-test")
	t.Setenv("TEST_ROUTE", "eu")
	r.cfg.APIKeyEnv = "TEST_OPENAI_KEY"
	r.cfg.ExtraHeaders = map[string]string{"X-Route": "TEST_ROUTE"}

	if err := r.exec(t, "parallel"); err != nil {
		t.Fatal(err)
	}
	steps := r.steps(t, "parallel")

	// user, call, call, result, result, assistant
	var callIDs []string
	for _, s := range steps {
		if c := s.GetToolCall(); c != nil {
			if !callIDPattern.MatchString(c.GetId()) {
				t.Errorf("logged call id %q is not a Conductor call_id", c.GetId())
			}
			callIDs = append(callIDs, c.GetId())
		}
	}
	if len(steps) != 6 || len(callIDs) != 2 {
		t.Fatalf("steps = %d, calls = %d; want 6 and 2", len(steps), len(callIDs))
	}

	// Each call reached its endpoint once, keyed by its call_id.
	if len(r.tools.requests) != 2 {
		t.Fatalf("tool requests = %+v, want 2", r.tools.requests)
	}
	for i, tr := range r.tools.requests {
		if tr.key != callIDs[i] {
			t.Errorf("tool request %d Idempotency-Key = %q, want %q", i, tr.key, callIDs[i])
		}
	}
	if r.tools.requests[0].body != `{"city":"Paris"}` || r.tools.requests[1].body != `{"city":"Oslo"}` {
		t.Errorf("tool bodies = %q, %q", r.tools.requests[0].body, r.tools.requests[1].body)
	}

	// The first request declares the tools and carries the configured headers.
	first := r.model.requests[0]
	if first.Model != "test-model" || first.ToolChoice != "auto" || len(first.Tools) != 2 ||
		first.Tools[0].Function.Description != "Weather for a city." || first.Tools[0].Function.Parameters["type"] != "object" {
		t.Errorf("first request = %+v", first)
	}
	if got := r.model.headers[0].Get("Authorization"); got != "Bearer sk-test" {
		t.Errorf("Authorization = %q", got)
	}
	if got := r.model.headers[0].Get("X-Route"); got != "eu" {
		t.Errorf("X-Route = %q", got)
	}

	// The second request replays both calls in one assistant message, and the
	// tool messages answer them by Conductor's call_id.
	msgs := r.model.requests[1].Messages
	if len(msgs) != 4 || msgs[1].Role != "assistant" || len(msgs[1].ToolCalls) != 2 {
		t.Fatalf("second request messages = %+v", msgs)
	}
	for i, tc := range msgs[1].ToolCalls {
		if tc.ID != callIDs[i] || msgs[2+i].Role != "tool" || msgs[2+i].ToolCallID != callIDs[i] {
			t.Errorf("call %d: id %q, tool message %+v; want call id %q", i, tc.ID, msgs[2+i], callIDs[i])
		}
	}
	if !strings.Contains(*msgs[2].Content, `"temp_c":21`) {
		t.Errorf("tool message content = %q", *msgs[2].Content)
	}

	if p, c := tokens(t, r.cfg, "prompt"), tokens(t, r.cfg, "completion"); p != 20 || c != 10 {
		t.Errorf("tokens = %v prompt, %v completion; want 20, 10", p, c)
	}
}

func TestOwnedLoop_MalformedArguments(t *testing.T) {
	const raw = `{"city": Par`
	r := newRig(t,
		toolCallsResponse("", "prov_a", "get_weather", raw),
		contentResponse("Sorry.", ""),
	)
	if err := r.exec(t, "malformed"); err != nil {
		t.Fatal(err)
	}
	steps := r.steps(t, "malformed")

	if len(r.tools.requests) != 0 {
		t.Errorf("tool called with malformed arguments: %+v", r.tools.requests)
	}
	result := steps[2].GetToolResult().GetFunctionResult()
	if !result.GetIsError() {
		t.Errorf("result = %v, want is_error", result)
	}
	msgs := r.model.requests[1].Messages
	if got := msgs[1].ToolCalls[0].Function.Arguments; got != raw {
		t.Errorf("replayed arguments = %q, want the model's own %q", got, raw)
	}
	if !strings.Contains(*msgs[2].Content, "not a JSON object") {
		t.Errorf("tool message content = %q", *msgs[2].Content)
	}
}

func TestOwnedLoop_ToolEndpoint5xx(t *testing.T) {
	r := newRig(t,
		toolCallsResponse("", "prov_a", "page_oncall", `{}`),
		contentResponse("Paging failed.", ""),
	)
	if err := r.exec(t, "down"); err != nil {
		t.Fatal(err)
	}
	steps := r.steps(t, "down")

	if len(r.tools.requests) != 1 || r.tools.requests[0].key != steps[1].GetToolCall().GetId() {
		t.Fatalf("tool requests = %+v", r.tools.requests)
	}
	result := steps[2].GetToolResult().GetFunctionResult()
	if !result.GetIsError() || result.GetResult().GetStructValue().GetFields()["status"].GetNumberValue() != 503 {
		t.Errorf("result = %v, want is_error with status 503", result)
	}
	if c := *r.model.requests[1].Messages[2].Content; !strings.Contains(c, "503") || !strings.Contains(c, "upstream down") {
		t.Errorf("tool message content = %q", c)
	}
}

func TestOwnedLoop_UsageMissing(t *testing.T) {
	r := newRig(t, contentResponse("Hi.", ""))
	if err := r.exec(t, "nousage"); err != nil {
		t.Fatal(err)
	}
	if steps := r.steps(t, "nousage"); steps[1].GetContent().GetRole() != "assistant" {
		t.Fatalf("steps = %v", steps)
	}
	if p := tokens(t, r.cfg, "prompt"); p != 0 {
		t.Errorf("prompt tokens = %v without a usage block", p)
	}
}

func TestOwnedLoop_MaxTurns(t *testing.T) {
	r := newRig(t, toolCallsResponse("", "prov_a", "get_weather", `{"city":"Paris"}`))
	r.cfg.MaxTurns = 1
	err := r.exec(t, "maxturns")
	if err == nil || !strings.Contains(err.Error(), "max_turns") {
		t.Fatalf("err = %v, want max_turns", err)
	}
	if len(r.model.requests) != 1 || len(r.tools.requests) != 1 {
		t.Errorf("model requests = %d, tool requests = %d; want 1 and 1", len(r.model.requests), len(r.tools.requests))
	}
}
