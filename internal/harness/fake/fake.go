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

// Package fake is a deterministic, zero-key harness for tests, demos and
// benchmarks. It follows the owned-loop turn protocol
// (docs/conductor/ARCHITECTURE.md §1.4): each Run receives the whole history
// and plays one turn of a script. Every tool effect is recorded in an
// EffectSink, so duplicate effects (invariant I2) are countable.
package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

// duplicateEffects is the runtime counterpart of I2's falsifying query: only
// the fake observes external effects, so only it can count a second one.
var duplicateEffects = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "conductor_duplicate_effects_total",
	Help: "Fake tool effects performed again for an idempotency key that already had one.",
}, []string{"tool"})

// CrashExitCode is the exit status of a crash_after_effect abort.
const CrashExitCode = 137

// Script is the plan a fake conversation plays. It is read from the request's
// agent_config; an empty config plays DefaultScript.
type Script struct {
	// Turns are the model turns, restarted on every user input.
	Turns []Turn `json:"turns"`
	// Tools sets per-tool behaviour, keyed by tool name.
	Tools map[string]Tool `json:"tools,omitempty"`
}

// Turn is one model turn.
type Turn struct {
	Thought   string     `json:"thought,omitempty"`
	Content   string     `json:"content,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	DelayMS   int        `json:"delay_ms,omitempty"`
	// FailOnce fails the first attempt of this turn in this process.
	FailOnce bool `json:"fail_once,omitempty"`
}

// ToolCall is a tool call a model turn emits.
type ToolCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args,omitempty"`
}

// Tool is how a tool behaves when a tool turn executes it.
type Tool struct {
	DelayMS int `json:"delay_ms,omitempty"`
	// FailOnce fails the first attempt of each call in this process, before
	// the effect.
	FailOnce bool `json:"fail_once,omitempty"`
	// CrashAfterEffect exits the process after the first effect for a key is
	// recorded and before its result is reported.
	CrashAfterEffect bool `json:"crash_after_effect,omitempty"`
	// DuplicateResult reports the result twice.
	DuplicateResult bool `json:"duplicate_result,omitempty"`
	// Idempotent makes the tool honour its idempotency key, as an idempotent
	// external API does: a repeat with a key that already had its effect
	// performs no new one. It models the external system, not the class
	// Conductor declares, so a tool declared IDEMPOTENT without it shows up
	// as a duplicate effect (I2).
	Idempotent bool `json:"idempotent,omitempty"`
}

// DefaultScript thinks, calls charge_card, calls send_email, then answers.
var DefaultScript = Script{
	Turns: []Turn{
		{Thought: "planning two steps", ToolCalls: []ToolCall{{Name: "charge_card", Args: map[string]any{"amount": 4200}}}},
		{ToolCalls: []ToolCall{{Name: "send_email", Args: map[string]any{"to": "user@example.com"}}}},
		{Content: "Charged and emailed."},
	},
	Tools: map[string]Tool{"charge_card": {Idempotent: true}},
}

// DefaultScriptTools are the recovery classes of DefaultScript's tools.
var DefaultScriptTools = map[string]string{"charge_card": "IDEMPOTENT", "send_email": "UNSAFE"}

// Harness is the fake harness.
type Harness struct {
	effects EffectSink
	exit    func(int)

	mu     sync.Mutex
	failed map[string]bool // fail_once keys already failed in this process
}

var _ harness.Harness = (*Harness)(nil)

// New returns a fake harness that records tool effects in effects.
func New(effects EffectSink) *Harness {
	return &Harness{effects: effects, exit: os.Exit, failed: map[string]bool{}}
}

// Start implements harness.Harness.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	script := DefaultScript
	if len(bytes.TrimSpace(config)) > 0 {
		script = Script{}
		dec := json.NewDecoder(bytes.NewReader(config))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&script); err != nil {
			return nil, fmt.Errorf("fake: parsing script from agent_config: %w", err)
		}
	}
	return &execution{h: h, conversationID: conversationID, id: uuid.NewString(), script: script}, nil
}

// failOnce reports whether key should fail now: true the first time only.
func (h *Harness) failOnce(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.failed[key] {
		return false
	}
	h.failed[key] = true
	return true
}

type execution struct {
	h              *Harness
	conversationID string
	id             string
	script         Script

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

// Run plays one turn: a tool turn if the history ends in unanswered tool
// calls, a model turn otherwise.
func (e *execution) Run(ctx context.Context, handler harness.Handler) error {
	e.mu.Lock()
	history := e.queued
	e.queued = nil
	e.mu.Unlock()
	if len(history) == 0 {
		return errors.New("fake: Run needs the conversation history queued")
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
	k := modelTurnIndex(history)
	if k >= len(e.script.Turns) {
		return fmt.Errorf("fake: script has %d turns, history needs turn %d", len(e.script.Turns), k+1)
	}
	t := e.script.Turns[k]
	if err := sleep(ctx, t.DelayMS); err != nil {
		return err
	}
	if t.FailOnce && e.h.failOnce(fmt.Sprintf("%s/turn/%d", e.conversationID, len(history))) {
		return fmt.Errorf("fake: turn %d failed once", k+1)
	}

	var steps []*proto.Step
	if t.Thought != "" {
		steps = append(steps, &proto.Step{Type: &proto.Step_Thought{Thought: &proto.ThoughtStep{
			Summary: []*proto.Content{textContent(t.Thought)},
		}}})
	}
	if t.Content != "" {
		steps = append(steps, &proto.Step{Type: &proto.Step_Content{Content: &proto.ContentStep{
			Role: "assistant", Content: []*proto.Content{textContent(t.Content)},
		}}})
	}
	for i, c := range t.ToolCalls {
		args, err := structpb.NewStruct(c.Args)
		if err != nil {
			return fmt.Errorf("fake: tool %q args: %w", c.Name, err)
		}
		steps = append(steps, &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
			// Derived from the conversation and history, so a replayed turn
			// yields the same id and no two conversations share one.
			Id: fmt.Sprintf("fake-%s-%d-%d", e.conversationID, len(history), i),
			Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{
				Name: c.Name, Arguments: args,
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

func (e *execution) toolTurn(ctx context.Context, handler harness.Handler, calls []*proto.ToolCallStep) error {
	for _, call := range calls {
		name := call.GetFunctionCall().GetName()
		tool := e.script.Tools[name]
		if err := sleep(ctx, tool.DelayMS); err != nil {
			return err
		}
		if tool.FailOnce && e.h.failOnce(e.conversationID+"/call/"+call.GetId()) {
			return fmt.Errorf("fake: tool %q failed once", name)
		}

		payload, err := call.GetFunctionCall().GetArguments().MarshalJSON()
		if err != nil {
			return fmt.Errorf("fake: tool %q args: %w", name, err)
		}
		// attempt is how many effects exist for the key once this call is done.
		attempt := 0
		if tool.Idempotent {
			if attempt, err = e.h.effects.Count(ctx, call.GetId()); err != nil {
				return fmt.Errorf("fake: counting effects of %q: %w", name, err)
			}
		}
		if attempt == 0 {
			attempt, err = e.h.effects.Record(ctx, Effect{
				CallID:         call.GetId(),
				IdempotencyKey: call.GetId(),
				ToolName:       name,
				Payload:        payload,
			})
			if err != nil {
				return fmt.Errorf("fake: recording effect of %q: %w", name, err)
			}
			if attempt > 1 {
				duplicateEffects.WithLabelValues(name).Inc()
			}
			if tool.CrashAfterEffect && attempt == 1 {
				e.h.exit(CrashExitCode)
			}
		}

		result, err := structpb.NewStruct(map[string]any{"status": "ok", "attempt": attempt})
		if err != nil {
			return err
		}
		step := &proto.Step{Type: &proto.Step_ToolResult{ToolResult: &proto.ToolResultStep{
			CallId: call.GetId(),
			Type: &proto.ToolResultStep_FunctionResult{FunctionResult: &proto.FunctionResultStep{
				Name:   name,
				Result: &proto.Value{Kind: &proto.Value_StructValue{StructValue: result}},
			}},
		}}}
		n := 1
		if tool.DuplicateResult {
			n = 2
		}
		for range n {
			if err := handler.OnMessage(ctx, e.id, step); err != nil {
				return err
			}
		}
	}
	return nil
}

// modelTurnIndex is the script turn the history is at: the number of tool
// result batches since the last user input.
func modelTurnIndex(history []*proto.Step) int {
	start := 0
	for i, s := range history {
		if s.GetContent().GetRole() == "user" {
			start = i + 1
		}
	}
	k, prevResult := 0, false
	for _, s := range history[start:] {
		isResult := s.GetToolResult() != nil
		if isResult && !prevResult {
			k++
		}
		prevResult = isResult
	}
	return k
}

func textContent(text string) *proto.Content {
	return &proto.Content{Type: &proto.Content_Text{Text: &proto.TextContent{Text: text}}}
}

func sleep(ctx context.Context, ms int) error {
	if ms <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(ms) * time.Millisecond):
		return nil
	}
}
