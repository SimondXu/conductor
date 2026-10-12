// Copyright 2026 Google LLC
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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

type fakeHarness struct{}

func (f *fakeHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return &fakeExecution{id: "fake-exec-id"}, nil
}

type fakeExecution struct {
	id     string
	queued []*proto.Step
}

func (f *fakeExecution) ID() string {
	return f.id
}

func (f *fakeExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	f.queued = append(f.queued, steps...)
	return nil
}

func (f *fakeExecution) Run(ctx context.Context, handler harness.Handler) error {
	step := &proto.Step{
		Type: &proto.Step_Content{
			Content: &proto.ContentStep{
				Role: "assistant",
				Content: []*proto.Content{
					{
						Type: &proto.Content_Text{
							Text: &proto.TextContent{
								Text: "Hello world",
							},
						},
					},
				},
			},
		},
	}
	if err := handler.OnMessage(ctx, f.id, step); err != nil {
		return err
	}
	return handler.OnComplete(ctx, f.id)
}

func (f *fakeExecution) Close(ctx context.Context) error {
	return nil
}

func TestController2_ExecHelloWorld(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("default-harness", &fakeHarness{}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("default-harness"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var outputs []*proto.Step
	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		outputs = append(outputs, resp.Outputs...)
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err != nil {
		t.Fatalf("Controller2.Exec failed: %v", err)
	}

	if len(outputs) != 1 {
		t.Fatalf("expected exactly 1 output message, got %d", len(outputs))
	}

	gotText := outputs[0].GetContent().Content[0].GetText().GetText()
	if gotText != "Hello world" {
		t.Errorf("expected 'Hello world' output text response, got %q", gotText)
	}

	// Verify that events were logged correctly in Conversation Log
	events, err := log.Events(ctx, cid)
	if err != nil {
		t.Fatalf("failed to retrieve logged events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 logged events, got %d", len(events))
	}

	// 1. First event should be inputs
	if len(events[0].Steps) != 1 {
		t.Errorf("expected 1 step in first event, got %d", len(events[0].Steps))
	} else {
		gotInputText := events[0].Steps[0].GetContent().Content[0].GetText().GetText()
		if gotInputText != "Trigger prompt" {
			t.Errorf("expected 'Trigger prompt' in logged input, got %q", gotInputText)
		}
	}
	if events[0].State != proto.State_STATE_PENDING {
		t.Errorf("expected first event state to be PENDING, got %v", events[0].State)
	}

	// 2. Second event should be output
	if len(events[1].Steps) != 1 {
		t.Errorf("expected 1 step in second event, got %d", len(events[1].Steps))
	} else {
		gotOutputText := events[1].Steps[0].GetContent().Content[0].GetText().GetText()
		if gotOutputText != "Hello world" {
			t.Errorf("expected 'Hello world' in logged output, got %q", gotOutputText)
		}
	}
	if events[1].State != proto.State_STATE_PENDING {
		t.Errorf("expected second event state to be PENDING, got %v", events[1].State)
	}

	// 3. Third event should be completion
	if len(events[2].Steps) != 0 {
		t.Errorf("expected 0 steps in third event, got %d", len(events[2].Steps))
	}
	if events[2].State != proto.State_STATE_COMPLETED {
		t.Errorf("expected third event state to be COMPLETED, got %v", events[2].State)
	}

}

func TestController2_ExecWithAgentID(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("my-agent", &fakeHarness{}); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var outputs []*proto.Step
	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		outputs = append(outputs, resp.Outputs...)
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "my-agent",
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err != nil {
		t.Fatalf("Controller2.Exec failed: %v", err)
	}

	if len(outputs) != 1 {
		t.Fatalf("expected exactly 1 output message, got %d", len(outputs))
	}

	gotText := outputs[0].GetContent().Content[0].GetText().GetText()
	if gotText != "Hello world" {
		t.Errorf("expected 'Hello world' output text response, got %q", gotText)
	}
}

func TestController2_ExecHarnessNotFound(t *testing.T) {
	ctx := context.Background()
	cid := "test-conversation-id"

	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry() // Empty registry, will force error for any requested agent

	c, err := New(ctx, Config{
		Registry: reg,
		EventLogBuilder: func() (eventlog.EventLog, error) {
			return log, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	handler := ExecHandler(func(resp *proto.CreateInteractionResponse) error {
		return nil
	})

	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "antigravity",
		Inputs:         []*proto.Step{harnesstest.UserStep("Trigger prompt")},
	}, handler)
	if err == nil {
		t.Fatal("expected error requesting unregistered agent, got nil")
	}
}

type testHarness struct {
	startCalls int
	startFunc  func(ctx context.Context, conversationID string) (harness.Execution, error)
}

func (c *testHarness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	c.startCalls++
	return c.startFunc(ctx, conversationID)
}

type testExecution struct {
	id         string
	queueCalls int
	runCalls   int
	closeCalls int
	queued     []*proto.Step
	runFunc    func(ctx context.Context, execID string, handler harness.Handler) error
}

func (c *testExecution) ID() string {
	return c.id
}

func (c *testExecution) Queue(ctx context.Context, steps ...*proto.Step) error {
	c.queueCalls++
	c.queued = append(c.queued, steps...)
	return nil
}

func (c *testExecution) Run(ctx context.Context, handler harness.Handler) error {
	c.runCalls++
	if c.runFunc != nil {
		return c.runFunc(ctx, c.id, handler)
	}
	return nil
}

func (c *testExecution) Close(ctx context.Context) error {
	c.closeCalls++
	return nil
}

func TestController2_ExecResumptionFlow(t *testing.T) {
	// Subtest 1: New Execution with Inputs
	t.Run("NewExecutionWithInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "new-conv"

		log := &eventlogtest.MemoryEventLog{}
		reg := NewRegistry()

		var exec *testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec = &testExecution{
					id: "exec-new",
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         []*proto.Step{harnesstest.UserStep("Hello")},
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		if h.startCalls != 1 {
			t.Errorf("expected 1 Start call, got %d", h.startCalls)
		}
		if exec.queueCalls != 1 {
			t.Errorf("expected 1 Queue call, got %d", exec.queueCalls)
		}
		if exec.runCalls != 1 {
			t.Errorf("expected 1 Run call, got %d", exec.runCalls)
		}
	})

	// Subtest 2: Pending Execution with NO New Inputs
	t.Run("PendingExecutionWithoutNewInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "pending-no-inputs"

		log := &eventlogtest.MemoryEventLog{}
		// Seed the event log with a pending event
		_, err := log.Append(ctx, &proto.StepEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			State:          proto.State_STATE_PENDING,
			Steps:          []*proto.Step{harnesstest.UserStep("Initial")},
		})
		if err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry()

		var exec *testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec = &testExecution{
					id: "exec-pending",
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         nil, // NO new inputs
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		if h.startCalls != 1 {
			t.Errorf("expected 1 Start call, got %d", h.startCalls)
		}
		if exec.queueCalls != 0 {
			t.Errorf("expected 0 Queue calls, got %d", exec.queueCalls)
		}
		if exec.runCalls != 1 {
			t.Errorf("expected 1 Run call, got %d", exec.runCalls)
		}
	})

	// Subtest 3: Pending Execution WITH New Inputs
	t.Run("PendingExecutionWithNewInputs", func(t *testing.T) {
		ctx := context.Background()
		cid := "pending-with-inputs"

		log := &eventlogtest.MemoryEventLog{}
		// Seed the event log with a pending event
		_, err := log.Append(ctx, &proto.StepEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			State:          proto.State_STATE_PENDING,
			Steps:          []*proto.Step{harnesstest.UserStep("Initial")},
		})
		if err != nil {
			t.Fatal(err)
		}

		reg := NewRegistry()

		var execs []*testExecution
		h := &testHarness{
			startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
				exec := &testExecution{
					id: fmt.Sprintf("exec-%d", len(execs)+1),
					runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
						return handler.OnComplete(ctx, execID)
					},
				}
				execs = append(execs, exec)
				return exec, nil
			},
		}
		if err := reg.RegisterHarness("test-agent", h); err != nil {
			t.Fatal(err)
		}

		c, err := New(ctx, Config{
			Registry:        reg,
			EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		err = c.Exec(ctx, &proto.CreateInteractionEvent{
			ConversationId: cid,
			AgentId:        "test-agent",
			Inputs:         []*proto.Step{harnesstest.UserStep("New input")},
		}, func(resp *proto.CreateInteractionResponse) error { return nil })
		if err != nil {
			t.Fatal(err)
		}

		// It should start the harness twice: once for resumption, once for new inputs.
		if h.startCalls != 2 {
			t.Errorf("expected 2 Start calls, got %d", h.startCalls)
		}
		if len(execs) != 2 {
			t.Fatalf("expected 2 execution sessions, got %d", len(execs))
		}

		// The first session (resumption) should NOT have queued inputs and should run.
		if execs[0].queueCalls != 0 {
			t.Errorf("expected first session to have 0 Queue calls, got %d", execs[0].queueCalls)
		}
		if execs[0].runCalls != 1 {
			t.Errorf("expected first session to have 1 Run call, got %d", execs[0].runCalls)
		}

		// The second session (new inputs) should have queued inputs and run.
		if execs[1].queueCalls != 1 {
			t.Errorf("expected second session to have 1 Queue call, got %d", execs[1].queueCalls)
		}
		if execs[1].runCalls != 1 {
			t.Errorf("expected second session to have 1 Run call, got %d", execs[1].runCalls)
		}
	})
}

// A conversation started on one harness must resume on that harness when the
// harness id is empty, not fall back to the registry default.
func TestExec_ResumeEmptyHarnessUsesStored(t *testing.T) {
	ctx := context.Background()
	cid := "resume-empty"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()

	def := &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}
	stored := &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}
	if err := reg.RegisterHarness("harness-a", def); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterHarness("harness-b", stored); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("harness-a"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	noop := ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })

	// Turn 1: explicitly run the NON-default harness.
	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-b",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, noop); err != nil {
		t.Fatalf("turn 1: %v", err)
	}

	// Turn 2: resume WITHOUT a harness id. Must reuse harness-b, not the default.
	before := stored.startCalls
	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("more")},
	}, noop); err != nil {
		t.Fatalf("turn 2 (resume, empty harness): %v", err)
	}
	if stored.startCalls <= before {
		t.Errorf("stored harness not used on resume (startCalls stayed %d)", stored.startCalls)
	}
	if def.startCalls != 0 {
		t.Errorf("default harness used on resume (startCalls=%d), want the stored harness", def.startCalls)
	}
}

// Verifies that an explicit harness change during resume is rejected.
func TestExec_ResumeExplicitDifferentHarnessRejected(t *testing.T) {
	ctx := context.Background()
	cid := "resume-mismatch"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("harness-a", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterHarness("harness-b", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	noop := ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-a",
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, noop); err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	err = c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		AgentId:        "harness-b",
		Inputs:         []*proto.Step{harnesstest.UserStep("more")},
	}, noop)
	if err == nil || !strings.Contains(err.Error(), "harness ID changed from harness-a to harness-b") {
		t.Fatalf("resume with a different harness: got %v, want error 'harness ID changed from harness-a to harness-b'", err)
	}
}

// Verifies that a new conversation started without a harness id records the
// default harness's canonical id, not "".
func TestExec_NewConversationLogsCanonicalDefault(t *testing.T) {
	ctx := context.Background()
	cid := "canonical-log"
	log := &eventlogtest.MemoryEventLog{}
	reg := NewRegistry()
	if err := reg.RegisterHarness("harness-a", &testHarness{startFunc: func(context.Context, string) (harness.Execution, error) {
		return &testExecution{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetDefaultHarness("harness-a"); err != nil {
		t.Fatal(err)
	}

	c, err := New(ctx, Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: cid,
		Inputs:         []*proto.Step{harnesstest.UserStep("hi")},
	}, ExecHandler(func(*proto.CreateInteractionResponse) error { return nil })); err != nil {
		t.Fatalf("exec: %v", err)
	}
	_, stored, _, err := newLogger(log, cid, "").ResumptionState(ctx)
	if err != nil {
		t.Fatalf("ResumptionState: %v", err)
	}
	if stored != "harness-a" {
		t.Errorf("logged harness id = %q, want canonical %q (not empty)", stored, "harness-a")
	}
}

func newOwnedLoopController(t *testing.T, log *eventlogtest.MemoryEventLog, effects *fake.MemoryEffects) *Controller {
	t.Helper()
	reg := NewRegistry()
	if err := reg.Register("fake", fake.New(effects), RecoveryFull); err != nil {
		t.Fatal(err)
	}
	c, err := New(context.Background(), Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestOwnedLoop_RunsScriptToCompletion(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	effects := &fake.MemoryEffects{}
	c := newOwnedLoopController(t, log, effects)

	var delivered int
	err := c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: "owned",
		AgentId:        "fake",
		Inputs:         []*proto.Step{harnesstest.UserStep("go")},
	}, func(resp *proto.CreateInteractionResponse) error {
		delivered += len(resp.Outputs)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	events, _ := log.Events(context.Background(), "owned")
	// inputs, thought, call, result, call, result, content, COMPLETED
	if len(events) != 8 {
		t.Fatalf("events = %d, want 8", len(events))
	}
	for i, ev := range events[:7] {
		if ev.State != proto.State_STATE_PENDING {
			t.Errorf("event %d state = %v, want PENDING", i, ev.State)
		}
	}
	if last := events[7]; last.State != proto.State_STATE_COMPLETED || len(last.Steps) != 0 {
		t.Errorf("last event = %v, want COMPLETED with no steps", last)
	}
	if delivered != 6 {
		t.Errorf("delivered %d steps, want 6", delivered)
	}
	if len(effects.Effects) != 2 {
		t.Errorf("effects = %d, want 2", len(effects.Effects))
	}
}

func TestOwnedLoop_RefusesUnansweredToolCall(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	effects := &fake.MemoryEffects{}
	ctx := context.Background()
	call := &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
		Id:   "c1",
		Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{Name: "charge_card"}},
	}}}
	if _, err := log.Append(ctx, &proto.StepEvent{
		ConversationId: "crashed", AgentId: "fake", State: proto.State_STATE_PENDING,
		Steps: []*proto.Step{harnesstest.UserStep("go"), call},
	}); err != nil {
		t.Fatal(err)
	}
	c := newOwnedLoopController(t, log, effects)

	err := c.Exec(ctx, &proto.CreateInteractionEvent{ConversationId: "crashed"}, nil)
	if err == nil || !strings.Contains(err.Error(), "unanswered tool call") {
		t.Fatalf("err = %v, want unanswered tool call refusal", err)
	}
	if len(effects.Effects) != 0 {
		t.Errorf("tool re-executed without the ledger: %+v", effects.Effects)
	}
}

func TestOwnedLoop_ResumesAfterFailedModelTurn(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	c := newOwnedLoopController(t, log, &fake.MemoryEffects{})
	ctx := context.Background()
	cfg := []byte(`{"turns":[{"content":"hello","fail_once":true}]}`)

	err := c.Exec(ctx, &proto.CreateInteractionEvent{
		ConversationId: "retry", AgentId: "fake", AgentConfig: cfg,
		Inputs: []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
	if err == nil {
		t.Fatal("first Exec: want the fail_once error")
	}
	if err := c.Exec(ctx, &proto.CreateInteractionEvent{ConversationId: "retry", AgentConfig: cfg}, nil); err != nil {
		t.Fatalf("resume: %v", err)
	}
	state, _, steps, err := newLogger(log, "retry", "").ResumptionState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state != proto.State_STATE_COMPLETED || len(steps) != 2 {
		t.Fatalf("state = %v, steps = %d; want COMPLETED with user + assistant", state, len(steps))
	}
}

// flakyLog fails Append on chosen calls (1-based) and delegates otherwise.
type flakyLog struct {
	eventlogtest.MemoryEventLog
	calls int
	fail  func(call int) bool
}

func (f *flakyLog) Append(ctx context.Context, ev *proto.StepEvent) (int64, error) {
	f.calls++
	if f.fail(f.calls) {
		return 0, errors.New("injected append failure")
	}
	return f.MemoryEventLog.Append(ctx, ev)
}

func newFlakyController(t *testing.T, log eventlog.EventLog, h harness.Harness) *Controller {
	t.Helper()
	reg := NewRegistry()
	if err := reg.RegisterHarness("h", h); err != nil {
		t.Fatal(err)
	}
	c, err := New(context.Background(), Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return log, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// I9: a step whose append failed is never delivered.
func TestAppendFailureIsNotDeliveredToClient(t *testing.T) {
	// Call 1 logs the inputs; every later append fails, retries included.
	log := &flakyLog{fail: func(call int) bool { return call > 1 }}
	c := newFlakyController(t, log, &fakeHarness{})

	var delivered int
	err := c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: "i9", AgentId: "h", Inputs: []*proto.Step{harnesstest.UserStep("hi")},
	}, func(r *proto.CreateInteractionResponse) error {
		delivered += len(r.Outputs)
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "injected append failure") {
		t.Fatalf("err = %v, want the append failure", err)
	}
	if delivered != 0 {
		t.Errorf("delivered %d steps that were never persisted", delivered)
	}
}

// A transient append failure is retried with the same event_id, so the step
// is persisted and delivered exactly once.
func TestTransientAppendFailureIsRetried(t *testing.T) {
	log := &flakyLog{fail: func(call int) bool { return call == 2 }}
	c := newFlakyController(t, log, &fakeHarness{})

	var delivered []*proto.Step
	if err := c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: "retry", AgentId: "h", Inputs: []*proto.Step{harnesstest.UserStep("hi")},
	}, func(r *proto.CreateInteractionResponse) error {
		delivered = append(delivered, r.Outputs...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	events, _ := log.Events(context.Background(), "retry")
	if len(delivered) != 1 || len(events) != 3 || delivered[0].Index != 2 {
		t.Fatalf("delivered %d (index %d), events %d; want 1 at index 2, 3 events", len(delivered), delivered[0].GetIndex(), len(events))
	}
}

// A failed turn records FAILED, so the next request with new inputs runs
// instead of first retrying the dead turn (streaming-path.md §5.1).
func TestFailedTurnDoesNotWedgeConversation(t *testing.T) {
	log := &eventlogtest.MemoryEventLog{}
	failing := true
	h := &testHarness{startFunc: func(ctx context.Context, conversationID string) (harness.Execution, error) {
		return &testExecution{id: "e", runFunc: func(ctx context.Context, execID string, handler harness.Handler) error {
			if failing {
				return errors.New("harness down")
			}
			return handler.OnComplete(ctx, execID)
		}}, nil
	}}
	c := newFlakyController(t, log, h)
	req := func() *proto.CreateInteractionEvent {
		return &proto.CreateInteractionEvent{ConversationId: "wedge", AgentId: "h", Inputs: []*proto.Step{harnesstest.UserStep("hi")}}
	}
	if err := c.Exec(context.Background(), req(), nil); err == nil {
		t.Fatal("first Exec: want error")
	}
	state, _, _, _ := newLogger(log, "wedge", "").ResumptionState(context.Background())
	if state != proto.State_STATE_FAILED {
		t.Fatalf("state after failure = %v, want FAILED", state)
	}
	failing = false
	if err := c.Exec(context.Background(), req(), nil); err != nil {
		t.Fatalf("second Exec: %v", err)
	}
	if h.startCalls != 2 {
		t.Errorf("Start calls = %d, want 2 (no resume of the failed turn)", h.startCalls)
	}
}
