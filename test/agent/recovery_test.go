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

package agent

import (
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/simondxu/conductor/internal/harness"
)

// followUp is a second task on the same conversation.
const followUp = "Order 1043 arrived damaged too. Do the same for it, one call per turn, waiting for each result: " +
	"look it up, refund the full amount it shows, then email the customer a one-line confirmation. Do not ask me anything."

// thanks ends a conversation with a turn that calls no tool.
const thanks = "That is all, thank you. Reply in one short sentence and do not call any tool."

// The replica that recovers the run dies at the same point, re-running the
// refund, and a third start completes it. OUTCOME_UNKNOWN is a durable
// resting state, so the second crash loses nothing, and the refund is
// started a third time under the same key (ARCHITECTURE.md §6.1, PRD.md §6.2
// step 5). Both kills come before the request is sent: one real refund.
func TestDoubleFault(t *testing.T) {
	r := newRig(t)
	r.start("a", "tool_started:refund_payment")
	r.send("a")
	r.waitExit("a")
	r.start("b", "tool_started:refund_payment")
	r.waitExit("b")
	r.start("a", "")
	r.check("COMPLETED", 1, 1)
	if got := r.attempts("refund_payment"); got != "3" {
		t.Errorf("refund_payment attempts = %s, want 3", got)
	}
	if _, _, reqs := r.tools.counts(); reqs["refund_payment"] != 1 {
		t.Errorf("refund_payment requests = %d, want 1", reqs["refund_payment"])
	}
	if g := r.query(`SELECT max(generation)::text FROM tool_calls WHERE conversation_id = $1`); g != "3" {
		t.Errorf("max generation = %s, want 3: one per owner", g)
	}
}

// The controller dies at each boundary of the UNSAFE email. Each row says
// what the docs require of the email.
func TestKillAtUnsafeBoundary(t *testing.T) {
	for _, tc := range []struct {
		at     string
		state  string
		emails int    // real emails, and email requests
		ledger string // the email's ledger row
	}{
		// PLANNED, never started: it cannot have had an effect, so the
		// resume runs it (ARCHITECTURE.md §6.1, recover.go).
		{"tool_planned:send_email", "COMPLETED", 1, "send_email:SUCCEEDED:1"},
		// STARTED but never sent. STARTED is written before the request,
		// so the ledger cannot tell this from a sent email: UNSAFE with an
		// unknown outcome is ABANDONED and the run fails (PRD.md §6.3).
		{"tool_started:send_email", "FAILED", 0, "send_email:ABANDONED:1"},
		// Settled: never run again.
		{"tool_settled:send_email", "COMPLETED", 1, "send_email:SUCCEEDED:1"},
	} {
		t.Run(strings.ReplaceAll(tc.at, ":", "_"), func(t *testing.T) {
			r := newRig(t)
			r.start("a", tc.at)
			r.send("a")
			r.waitExit("a")
			r.start("b", "")
			r.check(tc.state, 1, tc.emails)
			if _, _, reqs := r.tools.counts(); reqs["send_email"] != tc.emails {
				t.Errorf("send_email requests = %d, want %d", reqs["send_email"], tc.emails)
			}
			if got, want := r.ledger(), "lookup_order:SUCCEEDED:1 refund_payment:SUCCEEDED:1 "+tc.ledger; got != want {
				t.Errorf("ledger %s, want %s", got, want)
			}
			if tc.state != "FAILED" {
				return
			}
			// The abandoned email gets an error result in the same event as
			// FAILED (ARCHITECTURE.md §6.1), so the history stays answerable
			// and the conversation takes new input.
			evs := r.events()
			if last := evs[len(evs)-1]; last.State.String() != "STATE_FAILED" || len(last.Steps) != 1 ||
				!last.Steps[0].GetToolResult().GetFunctionResult().GetIsError() ||
				last.Steps[0].GetToolResult().GetCallId() != r.query(`SELECT call_id FROM tool_calls WHERE conversation_id = $1 AND tool_name = 'send_email'`) {
				t.Errorf("last event %v, want STATE_FAILED with the email's error result", last)
			}
			if out, code := r.client("b", "--input", thanks).wait(t); code != 0 {
				t.Fatalf("follow-up after the failed run: exit %d\n%s", code, out)
			}
			r.check("COMPLETED", 1, 0)
		})
	}
}

// The owner dies right after renewing its lease, so the lease it leaves has
// a whole ttl (3 s) to run. The other replica, already up, may take over
// only once PostgreSQL's clock says it expired (ARCHITECTURE.md §2.2), so
// the next model request cannot come sooner. A slow refund puts the first
// renewal, a third of the ttl in, inside the run.
func TestKillAfterLeaseRenewal(t *testing.T) {
	r := newRig(t)
	r.tools.slow, r.tools.delay = "refund_payment", 3*time.Second
	r.start("a", "lease_renewed")
	r.start("b", "")
	r.send("a")
	r.waitExit("a")
	killed := time.Now()
	r.check("COMPLETED", 1, 1)
	gap := model.firstAfter(killed).Sub(killed)
	t.Logf("first model request %s after the kill", gap.Round(time.Millisecond))
	if gap < 2500*time.Millisecond {
		t.Errorf("the first model request after the kill came %s later; the renewed lease had about 3 s left", gap.Round(time.Millisecond))
	}
	// With the mock the kill falls inside the refund; a real model is still
	// in its first turn.
	if got := r.attempts("refund_payment"); mode == "mock" && got != "2" {
		t.Errorf("refund_payment attempts = %s, want 2", got)
	}
}

// A second user turn on a completed conversation, with a crash in it. The
// recovering replica rebuilds the whole history, the first turn's thinking
// blocks included, and sends it back: the mock refuses a thinking block
// whose history changed, as the real API does (PRD.md §3.5,
// ARCHITECTURE.md §1.4).
func TestFollowUpTurn(t *testing.T) {
	for _, tc := range []struct {
		at       string
		attempts string // of the second refund
	}{
		// The first append of a fresh replica is the new input: the log
		// ends in it, and the other replica runs the whole turn.
		{"event_appended", "1"},
		{"tool_started:refund_payment", "2"},
	} {
		t.Run(strings.ReplaceAll(tc.at, ":", "_"), func(t *testing.T) {
			r := newRig(t)
			r.start("b", "")
			if out, code := r.send("b").wait(t); code != 0 {
				t.Fatalf("first turn: exit %d\n%s", code, out)
			}
			r.check("COMPLETED", 1, 1)
			step := r.lastStep()
			r.start("a", tc.at)
			r.client("a", "--input", followUp)
			r.waitExit("a")
			r.waitPast(step)
			r.check("COMPLETED", 2, 2)
			if got := r.attempts("refund_payment"); got != tc.attempts {
				t.Errorf("second refund_payment attempts = %s, want %s", got, tc.attempts)
			}
			if n := r.userInputs(); n != 2 {
				t.Errorf("%d user inputs logged, want 2", n)
			}
		})
	}
}

// New input while the run has an unanswered call is refused, so a tool
// result is never logged after a later user message (ARCHITECTURE.md §6.1,
// "New input over unanswered calls is refused"). Recovery is held off so
// the input meets the unfinished run rather than a resumed one.
func TestInputDuringUnfinishedRun(t *testing.T) {
	r := newRig(t)
	r.start("a", "tool_started:refund_payment")
	r.send("a")
	r.waitExit("a")
	r.db.Exec(`UPDATE runs SET next_attempt_at = now() + interval '1 hour' WHERE conversation_id = $1`, r.conv)
	r.start("b", "")
	for deadline := time.Now().Add(30 * time.Second); r.query(`SELECT (expires_at <= now())::text FROM conversation_leases WHERE conversation_id = $1`) != "true"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the dead owner's lease never expired")
		}
	}
	out, code := r.client("b", "--input", followUp).wait(t)
	if code == 0 || !strings.Contains(out, "resume it before sending new input") {
		t.Errorf("new input over an unanswered call: exit %d, want a refusal\n%s", code, out)
	}
	if n := r.userInputs(); n != 1 {
		t.Errorf("%d user inputs logged, want 1", n)
	}
	r.db.Exec(`UPDATE runs SET next_attempt_at = NULL WHERE conversation_id = $1`, r.conv)
	r.check("COMPLETED", 1, 1)
}

// A recovery that crashes the process every time is a poison run: after
// five recoveries the guard fails it and recovery stops (ARCHITECTURE.md
// §6.1, "Poison"). Every kill comes before the refund is sent, and no model
// request is made after the second turn. A failed run must still leave a
// history a later turn can answer: §6.1 writes an error result for each
// unanswered call with FAILED for that reason, and the I6 check requires a
// terminal run to rebuild to a closed history. So the conversation must
// take new input afterwards.
func TestRecoveryCrashLoop(t *testing.T) {
	r := newRig(t)
	stopSkipping := r.skipBackoff()
	const at = "tool_started:refund_payment"
	r.start("a", at)
	r.send("a")
	r.waitExit("a")
	deaths := 1
	r.start("a", at)
	for deadline := time.Now().Add(90 * time.Second); r.query(`SELECT state FROM runs WHERE conversation_id = $1`) == "PENDING"; {
		select {
		case <-r.procs["a"].done:
			deaths++
			r.start("a", at)
		case <-time.After(100 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("still PENDING after %d deaths\n%s", deaths, r.logs("a"))
		}
	}
	stopSkipping()
	r.signal("a", syscall.SIGKILL)
	refunds, emails, _ := r.tools.counts()
	state := r.query(`SELECT state FROM runs WHERE conversation_id = $1`)
	t.Logf("state %s after %d deaths; recovery_attempts %s; ledger %s", state, deaths,
		r.query(`SELECT recovery_attempts::text FROM runs WHERE conversation_id = $1`), r.ledger())
	if state != "FAILED" || deaths != 6 || refunds != 0 || emails != 0 {
		t.Fatalf("got %s after %d deaths with %d refund(s), %d email(s); want FAILED after 6 (1 + 5 recoveries), no effects", state, deaths, refunds, emails)
	}
	if n := r.modelRequests(); n != 2 && mode == "mock" {
		t.Errorf("%d model requests, want 2", n)
	}

	if open := harness.UnansweredToolCalls(r.history()); len(open) != 0 {
		t.Fatalf("the poisoned run left %d call(s) unanswered", len(open))
	}
	r.start("b", "")
	out, code := r.client("b", "--input", thanks).wait(t)
	out = ansi.ReplaceAllString(out, "")
	if code != 0 {
		t.Fatalf("follow-up after the poisoned run: exit %d\n%s", code, out)
	}
	r.check("COMPLETED", 0, 0)
}

// The model calls refund_payment and send_email in one turn. A tool turn
// runs every unanswered call of the history, one after the other, and all
// of them are STARTED before it runs (ARCHITECTURE.md §1.4, §3).
func TestParallelToolCalls(t *testing.T) {
	t.Run("no_failure", func(t *testing.T) {
		useScript(t, script{parallel: true})
		r := newRig(t)
		r.start("a", "")
		r.send("a")
		r.check("COMPLETED", 1, 1)
	})
	// The kill finds both calls STARTED and neither sent. The email's
	// outcome is unknown and UNSAFE, so the run fails, and every unanswered
	// call of the turn is closed: the IDEMPOTENT refund is abandoned too,
	// not retried (ARCHITECTURE.md §6.1, "When the run fails").
	t.Run("kill_at_tool_started_refund_payment", func(t *testing.T) {
		useScript(t, script{parallel: true})
		r := newRig(t)
		r.start("a", "tool_started:refund_payment")
		r.send("a")
		r.waitExit("a")
		r.start("b", "")
		r.check("FAILED", 0, 0)
		if got, want := r.ledger(), "lookup_order:SUCCEEDED:1 refund_payment:ABANDONED:1 send_email:ABANDONED:1"; got != want {
			t.Errorf("ledger %s, want %s", got, want)
		}
	})
	// The refund's result is logged, then the owner dies with the email in
	// flight. Recovery settles the refund from its logged result without
	// running it again (ARCHITECTURE.md §6.1, "A call answered in the log
	// but still STARTED"), and abandons the email.
	t.Run("killed_during_send_email", func(t *testing.T) {
		useScript(t, script{parallel: true})
		r := newRig(t)
		r.tools.slow, r.tools.delay = "send_email", 10*time.Second
		r.start("a", "")
		r.send("a")
		waitFor(t, r.tools.started, "the email request")
		r.signal("a", syscall.SIGKILL)
		r.start("b", "")
		r.check("FAILED", 1, 0)
		if got, want := r.ledger(), "lookup_order:SUCCEEDED:1 refund_payment:SUCCEEDED:1 send_email:ABANDONED:1"; got != want {
			t.Errorf("ledger %s, want %s", got, want)
		}
		if _, _, reqs := r.tools.counts(); reqs["refund_payment"] != 1 {
			t.Errorf("refund_payment requests = %d, want 1", reqs["refund_payment"])
		}
	})
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(60 * time.Second):
		t.Fatalf("%s never came", what)
	}
}
