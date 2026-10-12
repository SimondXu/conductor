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

package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/simondxu/conductor/internal/controller"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/internal/harness/ownedloop"
	"github.com/simondxu/conductor/proto"
)

// request is the part of a Messages request the tests inspect.
type request struct {
	Model        string            `json:"model"`
	MaxTokens    int               `json:"max_tokens"`
	Messages     []json.RawMessage `json:"messages"`
	Tools        []map[string]any  `json:"tools"`
	OutputConfig map[string]any    `json:"output_config"`
	CacheControl map[string]any    `json:"cache_control"`
}

type message struct {
	Role    string           `json:"role"`
	Content []map[string]any `json:"content"`
}

func (r request) message(t *testing.T, i int) message {
	t.Helper()
	var m message
	if err := json.Unmarshal(r.Messages[i], &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// modelServer is a fake Messages endpoint that serves scripted responses in
// order and records each request.
type modelServer struct {
	t         *testing.T
	responses []string
	status    int // when set, every response has this status

	mu       sync.Mutex
	requests []request
	headers  []http.Header
}

func (m *modelServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/messages" {
		m.t.Errorf("model request to %s", r.URL.Path)
	}
	var req request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		m.t.Errorf("decoding model request: %v", err)
	}
	m.mu.Lock()
	n := len(m.requests)
	m.requests = append(m.requests, req)
	m.headers = append(m.headers, r.Header.Clone())
	m.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if m.status != 0 {
		w.WriteHeader(m.status)
		io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"bad history"}}`)
		return
	}
	if n >= len(m.responses) {
		m.t.Errorf("unexpected model request %d", n+1)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	io.WriteString(w, m.responses[n])
}

type toolRequest struct {
	path, key, body string
}

// toolServer serves /weather with a JSON result.
type toolServer struct {
	mu       sync.Mutex
	requests []toolRequest
}

func (s *toolServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, toolRequest{r.URL.Path, r.Header.Get("Idempotency-Key"), string(body)})
	s.mu.Unlock()
	var args map[string]any
	json.Unmarshal(body, &args)
	json.NewEncoder(w).Encode(map[string]any{"city": args["city"], "temp_c": 21})
}

type rig struct {
	model *modelServer
	tools *toolServer
	log   *eventlogtest.MemoryEventLog
	cfg   Config
}

func newRig(t *testing.T, responses ...string) *rig {
	t.Helper()
	r := &rig{model: &modelServer{t: t, responses: responses}, tools: &toolServer{}, log: &eventlogtest.MemoryEventLog{}}
	ms := httptest.NewServer(r.model)
	ts := httptest.NewServer(r.tools)
	t.Cleanup(ms.Close)
	t.Cleanup(ts.Close)
	t.Setenv("TEST_ANTHROPIC_KEY", "sk-test")
	r.cfg = Config{
		BaseURL:   ms.URL,
		Model:     "test-model",
		APIKeyEnv: "TEST_ANTHROPIC_KEY",
		Tools: []Tool{{
			Name: "get_weather", Class: "IDEMPOTENT", Description: "Weather for a city.", URL: ts.URL + "/weather",
			Parameters: map[string]any{
				"type":       "object",
				"properties": map[string]any{"city": map[string]any{"type": "string"}},
				"required":   []any{"city"},
			},
		}},
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
	if err := reg.Register("anthropic", h, controller.RecoveryFull); err != nil {
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
		AgentId:        "anthropic",
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

func (r *rig) metric(t *testing.T, kind string) float64 {
	t.Helper()
	u, _ := url.Parse(r.cfg.BaseURL)
	if kind == "calls" {
		return testutil.ToFloat64(ownedloop.ModelCalls.WithLabelValues(u.Host, r.cfg.Model))
	}
	return testutil.ToFloat64(ownedloop.ModelTokens.WithLabelValues(u.Host, r.cfg.Model, kind))
}

const usage = `,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}`

// response is a Messages response with the given stop reason and blocks.
func response(stop, usage string, blocks ...string) string {
	return `{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[` +
		strings.Join(blocks, ",") + `],"stop_reason":"` + stop + `","stop_details":null` + usage + `}`
}

func thinking(sig string) string {
	return `{"type":"thinking","thinking":"","signature":"` + sig + `"}`
}

func text(s string) string { return `{"type":"text","text":"` + s + `"}` }

func toolUse(id, city string) string {
	return `{"type":"tool_use","id":"` + id + `","name":"get_weather","input":{"city":"` + city + `"}}`
}

var callIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func TestToolLoop_ThinkingAndParallelCalls(t *testing.T) {
	r := newRig(t,
		response("tool_use", usage, thinking("sig-1"), text("Checking both."), toolUse("toolu_a", "Paris"), toolUse("toolu_b", "Oslo")),
		response("end_turn", usage, text("Both 21C.")),
	)
	r.cfg.Effort = "high"
	// The SDK's own environment must not leak into the harness.
	t.Setenv("ANTHROPIC_API_KEY", "sk-wrong")
	t.Setenv("ANTHROPIC_BASE_URL", "http://127.0.0.1:1")

	if err := r.exec(t, "parallel"); err != nil {
		t.Fatal(err)
	}
	steps := r.steps(t, "parallel")

	// user, thought, text, call, call, result, result, text
	if len(steps) != 8 || string(steps[1].GetThought().GetSignature()) != "sig-1" {
		t.Fatalf("steps = %v", steps)
	}
	var callIDs []string
	for _, s := range steps {
		if c := s.GetToolCall(); c != nil {
			if !callIDPattern.MatchString(c.GetId()) {
				t.Errorf("logged call id %q is not a Conductor call_id", c.GetId())
			}
			callIDs = append(callIDs, c.GetId())
		}
	}
	if len(r.tools.requests) != 2 {
		t.Fatalf("tool requests = %+v, want 2", r.tools.requests)
	}
	for i, tr := range r.tools.requests {
		if tr.key != callIDs[i] {
			t.Errorf("tool request %d Idempotency-Key = %q, want %q", i, tr.key, callIDs[i])
		}
	}

	// The first request carries the configured key and parameters.
	h := r.model.headers[0]
	if h.Get("X-Api-Key") != "sk-test" || h.Get("Anthropic-Version") == "" {
		t.Errorf("headers: x-api-key %q, anthropic-version %q", h.Get("X-Api-Key"), h.Get("Anthropic-Version"))
	}
	first := r.model.requests[0]
	if first.Model != "test-model" || first.MaxTokens != DefaultMaxTokens || first.OutputConfig["effort"] != "high" ||
		first.CacheControl["type"] != "ephemeral" {
		t.Errorf("first request = %+v", first)
	}
	schema, _ := first.Tools[0]["input_schema"].(map[string]any)
	if len(first.Tools) != 1 || first.Tools[0]["description"] != "Weather for a city." ||
		schema["type"] != "object" || schema["properties"] == nil || len(schema["required"].([]any)) != 1 {
		t.Errorf("tools = %+v", first.Tools)
	}

	// The second request sends the model turn back as it came, in one
	// assistant message, with Conductor's call_ids, and answers both calls in
	// one user message.
	second := r.model.requests[1]
	if len(second.Messages) != 3 {
		t.Fatalf("second request has %d messages, want 3", len(second.Messages))
	}
	asst := second.message(t, 1)
	if asst.Role != "assistant" || len(asst.Content) != 4 ||
		asst.Content[0]["type"] != "thinking" || asst.Content[0]["signature"] != "sig-1" ||
		asst.Content[1]["text"] != "Checking both." ||
		asst.Content[2]["id"] != callIDs[0] || asst.Content[3]["id"] != callIDs[1] {
		t.Errorf("assistant message = %+v", asst)
	}
	if in, _ := asst.Content[2]["input"].(map[string]any); in["city"] != "Paris" {
		t.Errorf("tool_use input = %v", asst.Content[2]["input"])
	}
	results := second.message(t, 2)
	if results.Role != "user" || len(results.Content) != 2 ||
		results.Content[0]["tool_use_id"] != callIDs[0] || results.Content[1]["tool_use_id"] != callIDs[1] ||
		!strings.Contains(fmt.Sprint(results.Content[0]["content"]), `"temp_c":21`) {
		t.Errorf("tool results = %+v", results)
	}

	// prompt counts cache reads and writes: (10+3+2) * 2.
	if p, c, n := r.metric(t, "prompt"), r.metric(t, "completion"), r.metric(t, "calls"); p != 30 || c != 10 || n != 2 {
		t.Errorf("prompt %v, completion %v, calls %v; want 30, 10, 2", p, c, n)
	}
}

// Every request's messages must be a byte-for-byte prefix of the next
// request's, or the API invalidates the earlier thinking blocks.
func TestToolLoop_HistoryIsAppendOnly(t *testing.T) {
	r := newRig(t,
		response("tool_use", usage, thinking("sig-1"), toolUse("toolu_a", "Paris")),
		response("tool_use", usage, thinking("sig-2"), text("Now Oslo."), toolUse("toolu_b", "Oslo")),
		response("end_turn", usage, thinking("sig-3"), text("Both 21C.")),
	)
	if err := r.exec(t, "appendonly"); err != nil {
		t.Fatal(err)
	}
	r.steps(t, "appendonly")
	reqs := r.model.requests
	if len(reqs) != 3 {
		t.Fatalf("model requests = %d, want 3", len(reqs))
	}
	for i := 1; i < len(reqs); i++ {
		prev, next := reqs[i-1].Messages, reqs[i].Messages
		if len(next) <= len(prev) {
			t.Fatalf("request %d has %d messages, request %d has %d", i, len(prev), i+1, len(next))
		}
		for j := range prev {
			if !bytes.Equal(prev[j], next[j]) {
				t.Errorf("request %d message %d changed:\n%s\n%s", i+1, j, prev[j], next[j])
			}
		}
	}
}

func TestModelTurn_Refusal(t *testing.T) {
	r := newRig(t, `{"id":"msg_1","type":"message","role":"assistant","model":"test-model","content":[],"stop_reason":"refusal",`+
		`"stop_details":{"type":"refusal","category":"cyber","explanation":"declined"}`+usage+`}`)
	err := r.exec(t, "refusal")
	if err == nil || !strings.Contains(err.Error(), "refused") || !strings.Contains(err.Error(), "cyber") {
		t.Fatalf("err = %v, want a refusal naming its category", err)
	}
}

func TestModelTurn_MaxTokens(t *testing.T) {
	r := newRig(t, response("max_tokens", usage, text("Half an ans")))
	err := r.exec(t, "maxtokens")
	if err == nil || !strings.Contains(err.Error(), "max_tokens") {
		t.Fatalf("err = %v, want max_tokens", err)
	}
}

func TestModelTurn_UsageMissing(t *testing.T) {
	r := newRig(t, response("end_turn", "", text("Hi.")))
	if err := r.exec(t, "nousage"); err != nil {
		t.Fatal(err)
	}
	if steps := r.steps(t, "nousage"); steps[1].GetContent().GetRole() != "assistant" {
		t.Fatalf("steps = %v", steps)
	}
	if p, n := r.metric(t, "prompt"), r.metric(t, "calls"); p != 0 || n != 1 {
		t.Errorf("prompt tokens %v, calls %v; want 0 and 1", p, n)
	}
}

func TestModelTurn_HTTPError(t *testing.T) {
	r := newRig(t)
	r.model.status = http.StatusBadRequest
	err := r.exec(t, "badrequest")
	if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "bad history") {
		t.Fatalf("err = %v, want the endpoint's 400", err)
	}
	if len(r.model.requests) != 1 {
		t.Errorf("model requests = %d; a 400 must not be retried", len(r.model.requests))
	}
}
