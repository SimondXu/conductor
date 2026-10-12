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

package controller

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/eventlog/eventlogtest"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/fake"
	"github.com/simondxu/conductor/internal/harness/harnesstest"
	"github.com/simondxu/conductor/proto"
)

// ledgerRig is a controller with a Postgres ledger (PG_TEST_DSN), an
// in-memory event log and a fake harness, possibly wrapped.
type ledgerRig struct {
	c       *Controller
	log     *eventlogtest.MemoryEventLog
	ledger  *toolledger.Postgres
	effects *fake.MemoryEffects
	conv    string
}

func newLedgerRig(t *testing.T, wrap func(harness.Harness) harness.Harness) *ledgerRig {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping tool ledger integration tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	ledger, err := toolledger.OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ledger.Close() })

	r := &ledgerRig{log: &eventlogtest.MemoryEventLog{}, ledger: ledger, effects: &fake.MemoryEffects{}, conv: t.Name() + "-" + uuid.NewString()}
	var h harness.Harness = fake.New(r.effects)
	if wrap != nil {
		h = wrap(h)
	}
	reg := NewRegistry()
	if err := reg.Register("fake", h, RecoveryFull); err != nil {
		t.Fatal(err)
	}
	reg.DeclareTools("fake", map[string]toolledger.Class{"charge_card": toolledger.Idempotent})
	r.c, err = New(context.Background(), Config{
		Registry:        reg,
		EventLogBuilder: func() (eventlog.EventLog, error) { return r.log, nil },
		Ledger:          ledger,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.c.Close() })
	return r
}

func (r *ledgerRig) exec(cfg string) error {
	return r.c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: r.conv, AgentId: "fake", AgentConfig: []byte(cfg),
		Inputs: []*proto.Step{harnesstest.UserStep("go")},
	}, nil)
}

// resume resumes the conversation with no input and no agent_config.
func (r *ledgerRig) resume() error {
	return r.c.Exec(context.Background(), &proto.CreateInteractionEvent{ConversationId: r.conv}, nil)
}

func (r *ledgerRig) wantState(t *testing.T, want proto.State) {
	t.Helper()
	if got := rebuilt(t, r.log, r.conv).State; got != want {
		t.Fatalf("run state = %v, want %v", got, want)
	}
}

func (r *ledgerRig) calls(t *testing.T) []toolledger.ToolCall {
	t.Helper()
	calls, err := r.ledger.Calls(context.Background(), r.conv)
	if err != nil {
		t.Fatal(err)
	}
	return calls
}

// loggedSteps returns the tool call ids and tool result call ids in the log.
func (r *ledgerRig) loggedSteps(t *testing.T) (callIDs, resultIDs []string) {
	t.Helper()
	events, _ := r.log.Events(context.Background(), r.conv)
	for _, ev := range events {
		for _, s := range ev.Steps {
			if c := s.GetToolCall(); c != nil {
				callIDs = append(callIDs, c.GetId())
			}
			if res := s.GetToolResult(); res != nil {
				resultIDs = append(resultIDs, res.GetCallId())
			}
		}
	}
	return callIDs, resultIDs
}

// The durable state alone says which tools ran, with what arguments, what
// they returned, how many attempts, and how each ended (M3 DoD), and it
// agrees with the log (I7b).
func TestLedger_RecordsEveryCall(t *testing.T) {
	r := newLedgerRig(t, nil)
	if err := r.exec(""); err != nil {
		t.Fatal(err)
	}
	calls := r.calls(t)
	if len(calls) != 2 {
		t.Fatalf("ledger has %d calls, want 2", len(calls))
	}
	want := []struct {
		tool  string
		class toolledger.Class
		args  string
	}{
		{"charge_card", toolledger.Idempotent, `{"amount": 4200}`},
		{"send_email", toolledger.Unsafe, `{"to": "user@example.com"}`}, // undeclared: UNSAFE
	}
	callIDs, resultIDs := r.loggedSteps(t)
	for i, c := range calls {
		if c.ToolName != want[i].tool || c.Class != want[i].class || string(c.Args) != want[i].args {
			t.Errorf("call %d = %s %s %s, want %+v", i, c.ToolName, c.Class, c.Args, want[i])
		}
		if c.State != toolledger.Succeeded || c.Attempts != 1 || !strings.Contains(string(c.Result), `"status": "ok"`) {
			t.Errorf("call %d: state %s attempts %d result %s", i, c.State, c.Attempts, c.Result)
		}
		if c.CallID != callIDs[i] || c.CallID != resultIDs[i] || c.IdempotencyKey != c.CallID {
			t.Errorf("call %d id %s; log call %s, log result %s, key %s", i, c.CallID, callIDs[i], resultIDs[i], c.IdempotencyKey)
		}
		if r.effects.Effects[i].IdempotencyKey != c.CallID {
			t.Errorf("effect %d key %s, want the ledger call_id %s", i, r.effects.Effects[i].IdempotencyKey, c.CallID)
		}
	}
}

// A tool turn that fails before the effect still cannot prove it: the call
// is OUTCOME_UNKNOWN with one attempt. It is IDEMPOTENT, so a resume runs it
// again under the same key and the run completes.
func TestLedger_FailedToolTurnIsOutcomeUnknown(t *testing.T) {
	r := newLedgerRig(t, nil)
	err := r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"fail_once":true}}}`)
	if err == nil {
		t.Fatal("want the fail_once error")
	}
	c := r.calls(t)[0]
	if c.State != toolledger.OutcomeUnknown || c.Attempts != 1 || !strings.Contains(c.Error, "failed once") {
		t.Fatalf("call = %s attempts %d error %q", c.State, c.Attempts, c.Error)
	}
	if len(r.effects.Effects) != 0 {
		t.Fatalf("effects = %d, want 0", len(r.effects.Effects))
	}
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if c := r.calls(t)[0]; c.State != toolledger.Succeeded || c.Attempts != 2 {
		t.Fatalf("after resume call = %s attempts %d, want SUCCEEDED after 2", c.State, c.Attempts)
	}
	if len(r.effects.Effects) != 1 {
		t.Fatalf("effects = %d, want 1", len(r.effects.Effects))
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
}

// The crash-like case: the effect happened but its result never arrived.
// The ledger records the call as OUTCOME_UNKNOWN; the log has the call and no
// result; the effect sink shows the effect.
func TestLedger_LostResultIsOutcomeUnknown(t *testing.T) {
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return &lossyHarness{Harness: h} })
	err := r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}]}`)
	if err == nil || !strings.Contains(err.Error(), "connection lost") {
		t.Fatalf("err = %v, want connection lost", err)
	}
	c := r.calls(t)[0]
	if c.State != toolledger.OutcomeUnknown || c.Attempts != 1 || !strings.Contains(c.Error, "connection lost") {
		t.Fatalf("call = %s attempts %d error %q", c.State, c.Attempts, c.Error)
	}
	callIDs, resultIDs := r.loggedSteps(t)
	if len(callIDs) != 1 || len(resultIDs) != 0 {
		t.Fatalf("log calls %v results %v; want one call, no result", callIDs, resultIDs)
	}
	if len(r.effects.Effects) != 1 || r.effects.Effects[0].IdempotencyKey != c.CallID {
		t.Fatalf("effects = %+v", r.effects.Effects)
	}
}

// A duplicated result callback settles the call once and does not fail.
func TestLedger_DuplicateResultSettlesOnce(t *testing.T) {
	r := newLedgerRig(t, nil)
	if err := r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"duplicate_result":true}}}`); err != nil {
		t.Fatal(err)
	}
	if c := r.calls(t)[0]; c.State != toolledger.Succeeded || c.Attempts != 1 {
		t.Fatalf("call = %s attempts %d", c.State, c.Attempts)
	}
}

// lossyHarness runs tool turns but loses their results and reports a broken
// connection, as a harness that dies after the effect would. With once, only
// the first tool turn is lossy.
type lossyHarness struct {
	harness.Harness
	once    bool
	dropped atomic.Bool
}

func (l *lossyHarness) Start(ctx context.Context, conv string, cfg []byte) (harness.Execution, error) {
	e, err := l.Harness.Start(ctx, conv, cfg)
	return lossyExecution{Execution: e, h: l}, err
}

type lossyExecution struct {
	harness.Execution
	h *lossyHarness
}

func (l lossyExecution) Run(ctx context.Context, h harness.Handler) error {
	if l.h.once && l.h.dropped.Load() {
		return l.Execution.Run(ctx, h)
	}
	d := &dropResults{Handler: h}
	if err := l.Execution.Run(ctx, d); err != nil {
		return err
	}
	if d.dropped {
		l.h.dropped.Store(true)
		return errors.New("connection lost")
	}
	return nil
}

type dropResults struct {
	harness.Handler
	dropped bool
}

func (d *dropResults) OnMessage(ctx context.Context, id string, s *proto.Step) error {
	if s.GetToolResult() != nil {
		d.dropped = true
		return nil
	}
	return d.Handler.OnMessage(ctx, id, s)
}

// The effect of an IDEMPOTENT call happened but its result was lost. A
// resume runs it again under the same idempotency key: two attempts, one
// effect (I2), and the run completes with the result in the log (I7b).
func TestRecover_IdempotentLostResultIsRerun(t *testing.T) {
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return &lossyHarness{Harness: h, once: true} })
	if err := r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`); err == nil {
		t.Fatal("want connection lost")
	}
	err := r.c.Exec(context.Background(), &proto.CreateInteractionEvent{
		ConversationId: r.conv, Inputs: []*proto.Step{harnesstest.UserStep("again")},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "unfinished run") {
		t.Fatalf("new input over an unanswered call: err = %v, want unfinished run", err)
	}
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	c := r.calls(t)[0]
	if c.State != toolledger.Succeeded || c.Attempts != 2 {
		t.Fatalf("call = %s attempts %d, want SUCCEEDED after 2", c.State, c.Attempts)
	}
	if len(r.effects.Effects) != 1 {
		t.Fatalf("effects = %d, want 1 per idempotency key", len(r.effects.Effects))
	}
	if _, results := r.loggedSteps(t); len(results) != 1 || results[0] != c.CallID {
		t.Fatalf("logged results %v, want one for %s", results, c.CallID)
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
}

// An UNSAFE call whose outcome is unknown is never run again: it is
// ABANDONED, the log gets an error result and FAILED in one event, and a
// later resume changes nothing.
func TestRecover_UnsafeUnknownIsAbandoned(t *testing.T) {
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return &lossyHarness{Harness: h, once: true} })
	if err := r.exec(`{"turns":[{"tool_calls":[{"name":"send_email"}]},{"content":"done"}]}`); err == nil {
		t.Fatal("want connection lost")
	}
	err := r.resume()
	if err == nil || !strings.Contains(err.Error(), "run failed") {
		t.Fatalf("resume: err = %v, want run failed", err)
	}
	c := r.calls(t)[0]
	if c.State != toolledger.Abandoned || c.Attempts != 1 || c.Error != errAbandoned {
		t.Fatalf("call = %s attempts %d error %q, want ABANDONED after 1", c.State, c.Attempts, c.Error)
	}
	events, _ := r.log.Events(context.Background(), r.conv)
	last := events[len(events)-1]
	if last.State != proto.State_STATE_FAILED || len(last.Steps) != 1 ||
		last.Steps[0].GetToolResult().GetCallId() != c.CallID || !last.Steps[0].GetToolResult().GetFunctionResult().GetIsError() {
		t.Fatalf("last event = %v, want FAILED with an error result for %s", last, c.CallID)
	}
	if err := r.resume(); err != nil {
		t.Fatalf("second resume: %v", err)
	}
	if len(r.effects.Effects) != 1 || r.calls(t)[0].Attempts != 1 {
		t.Fatalf("UNSAFE call re-run: effects %d attempts %d", len(r.effects.Effects), r.calls(t)[0].Attempts)
	}
}

// The owner dies inside the tool turn, after the effect and before anything
// else is written: the call is left STARTED. A resume marks it
// OUTCOME_UNKNOWN, then runs it again because it is IDEMPOTENT.
func TestRecover_CrashInToolTurnLeavesStarted(t *testing.T) {
	var recovered atomic.Bool
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return crashingHarness{h, &recovered, false} })
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.exec(`{"turns":[{"tool_calls":[{"name":"charge_card"}]},{"content":"done"}],"tools":{"charge_card":{"idempotent":true}}}`)
	}()
	<-done
	if c := r.calls(t)[0]; c.State != toolledger.Started {
		t.Fatalf("after the crash call = %s, want STARTED", c.State)
	}
	recovered.Store(true)
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	c := r.calls(t)[0]
	if c.State != toolledger.Succeeded || c.Attempts != 2 || len(r.effects.Effects) != 1 {
		t.Fatalf("call = %s attempts %d effects %d, want SUCCEEDED after 2 with 1 effect", c.State, c.Attempts, len(r.effects.Effects))
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
}

// The owner dies after the tool result is logged and before the call is
// settled: the call is STARTED but answered in the log. A resume settles it
// from the logged result without running it again, and completes the run.
func TestRecover_CrashAfterResultLoggedSettlesFromLog(t *testing.T) {
	var recovered atomic.Bool
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return crashingHarness{h, &recovered, true} })
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.exec(`{"turns":[{"tool_calls":[{"name":"send_email"}]},{"content":"done"}]}`)
	}()
	<-done
	c := r.calls(t)[0]
	if _, results := r.loggedSteps(t); c.State != toolledger.Started || len(results) != 1 {
		t.Fatalf("after the crash call = %s with %d logged results, want STARTED with 1", c.State, len(results))
	}
	recovered.Store(true)
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	c = r.calls(t)[0]
	if c.State != toolledger.Succeeded || c.Attempts != 1 || len(r.effects.Effects) != 1 {
		t.Fatalf("call = %s attempts %d effects %d, want SUCCEEDED after 1 with 1 effect", c.State, c.Attempts, len(r.effects.Effects))
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
}

// The owner died after logging a call and before planning it: the ledger has
// no row. A resume plans it under its logged call_id and runs it.
func TestRecover_UnplannedCallIsPlannedAndRun(t *testing.T) {
	r := newLedgerRig(t, nil)
	call := &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
		Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{Name: "charge_card"}},
	}}}
	p, err := plannedCall(r.conv, 1, call.GetToolCall())
	if err != nil {
		t.Fatal(err)
	}
	call.GetToolCall().Id = p.CallID
	cfg, _ := structpb.NewStruct(map[string]any{"turns": []any{
		map[string]any{"tool_calls": []any{map[string]any{"name": "charge_card"}}},
		map[string]any{"content": "done"},
	}})
	if _, err := r.log.Append(context.Background(), &proto.StepEvent{
		ConversationId: r.conv, AgentId: "fake", AgentConfig: cfg, State: proto.State_STATE_PENDING,
		Steps: []*proto.Step{harnesstest.UserStep("go"), call},
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	calls := r.calls(t)
	if len(calls) != 1 || calls[0].CallID != p.CallID || calls[0].State != toolledger.Succeeded || calls[0].Attempts != 1 {
		t.Fatalf("calls = %+v, want %s SUCCEEDED after 1", calls, p.CallID)
	}
	r.wantState(t, proto.State_STATE_COMPLETED)
}

// crashingHarness ends the calling goroutine when a tool turn reports a
// result, after the effect, until recovered is set: like a process dying
// there, nothing after it runs except deferred calls. With afterLog it dies
// once the result is logged, otherwise before.
type crashingHarness struct {
	harness.Harness
	recovered *atomic.Bool
	afterLog  bool
}

func (c crashingHarness) Start(ctx context.Context, conv string, cfg []byte) (harness.Execution, error) {
	e, err := c.Harness.Start(ctx, conv, cfg)
	return crashingExecution{e, c}, err
}

type crashingExecution struct {
	harness.Execution
	c crashingHarness
}

func (e crashingExecution) Run(ctx context.Context, h harness.Handler) error {
	return e.Execution.Run(ctx, crashOnResult{h, e.c})
}

type crashOnResult struct {
	harness.Handler
	c crashingHarness
}

func (c crashOnResult) OnMessage(ctx context.Context, id string, s *proto.Step) error {
	if s.GetToolResult() == nil || c.c.recovered.Load() {
		return c.Handler.OnMessage(ctx, id, s)
	}
	if c.c.afterLog {
		_ = c.Handler.OnMessage(ctx, id, s)
	}
	runtime.Goexit()
	return nil
}
