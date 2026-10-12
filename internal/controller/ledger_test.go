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
	"strings"
	"testing"

	"github.com/google/uuid"

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
// is OUTCOME_UNKNOWN with one attempt, and a resume does not re-run it.
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
	if err := r.c.Exec(context.Background(), &proto.CreateInteractionEvent{ConversationId: r.conv}, nil); err == nil {
		t.Fatal("resume: want refusal while the call is unsettled")
	}
	if len(r.effects.Effects) != 0 || r.calls(t)[0].Attempts != 1 {
		t.Fatal("resume re-ran the tool")
	}
}

// The crash-like case: the effect happened but its result never arrived.
// The ledger records the call as OUTCOME_UNKNOWN; the log has the call and no
// result; the effect sink shows the effect.
func TestLedger_LostResultIsOutcomeUnknown(t *testing.T) {
	r := newLedgerRig(t, func(h harness.Harness) harness.Harness { return lossyHarness{h} })
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
// connection, as a harness that dies after the effect would.
type lossyHarness struct{ harness.Harness }

func (l lossyHarness) Start(ctx context.Context, conv string, cfg []byte) (harness.Execution, error) {
	e, err := l.Harness.Start(ctx, conv, cfg)
	return lossyExecution{Execution: e}, err
}

type lossyExecution struct{ harness.Execution }

func (l lossyExecution) Run(ctx context.Context, h harness.Handler) error {
	d := &dropResults{Handler: h}
	if err := l.Execution.Run(ctx, d); err != nil {
		return err
	}
	if d.dropped {
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
