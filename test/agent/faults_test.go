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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The refund endpoint answers 503 once. The model sees an is_error result
// and calls the tool again (PRD.md §3.2: "a non-2xx response ... becomes a
// result with is_error set, which the model sees on its next turn"). The
// failed call is settled FAILED and the retry is a call of its own.
func TestToolErrorThenRetry(t *testing.T) {
	const taskRetry = task + " If a tool returns an error, call it once more with the same arguments."
	for _, tc := range []struct {
		name        string
		afterEffect bool
		refunds     int
	}{
		{"503_before_effect", false, 1},
		// The endpoint refunds, then answers 503. The retry is a new call
		// with a new call_id, so the endpoint's Idempotency-Key cannot
		// catch it and the customer is refunded twice. PRD.md §3.3 states
		// this limit; I2 holds, since it is per key.
		{"503_after_effect", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			r.tools.fail, r.tools.failAfterEffect = "refund_payment", tc.afterEffect
			r.start("a", "")
			r.client("a", "--input", taskRetry)
			r.check("COMPLETED", tc.refunds, 1)
			if got, want := r.ledger(), "lookup_order:SUCCEEDED:1 refund_payment:FAILED:1 refund_payment:SUCCEEDED:1 send_email:SUCCEEDED:1"; got != want {
				t.Errorf("ledger %s, want %s", got, want)
			}
			if _, _, reqs := r.tools.counts(); reqs["refund_payment"] != 2 {
				t.Errorf("refund_payment requests = %d, want 2", reqs["refund_payment"])
			}
		})
	}
}

// The model endpoint fails a turn, then recovers. A model request has no
// side effects, so it is retried: by the SDK, twice (PRD.md §3.5), and past
// that by recovery, since a turn whose request fails logs nothing and the
// run stays PENDING (ARCHITECTURE.md §1.4, "As built"). The tool calls
// before it are not run again.
func TestModelTransientErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		s          script
		extra      int    // failed model requests
		recoveries string // recovery_attempts
	}{
		{"529_once", script{failAfter: "lookup_order", failWith: "529", failTimes: 1}, 1, "0"},
		{"500_three_times", script{failAfter: "refund_payment", failWith: "500", failTimes: 3}, 3, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useScript(t, tc.s)
			r := newRig(t)
			r.start("a", "")
			r.send("a")
			r.check("COMPLETED", 1, 1)
			if got := r.modelRequests(); got != 4+tc.extra {
				t.Errorf("%d model requests, want %d", got, 4+tc.extra)
			}
			if _, _, reqs := r.tools.counts(); reqs["refund_payment"] != 1 {
				t.Errorf("refund_payment requests = %d, want 1", reqs["refund_payment"])
			}
			if got := r.query(`SELECT recovery_attempts::text FROM runs WHERE conversation_id = $1`); got != tc.recoveries {
				t.Errorf("recovery_attempts = %s, want %s", got, tc.recoveries)
			}
		})
	}
}

// The model turn after the refund fails every time. Nothing of a failed
// turn is logged: a 400 has no body to log, and a refusal or a max_tokens
// stop fails the turn rather than log a partial answer or a cut-off tool
// call (PRD.md §3.5). The run stays PENDING, each recovery fails the same
// way, and after five the poison guard fails the run (ARCHITECTURE.md
// §6.1), so the spend is bounded: the client's turn plus five recoveries.
// A model that never stops calling tools is bounded by max_turns (24),
// counted from the history (PRD.md §3.2), so a recovery does not grant it
// more turns.
func TestModelTurnFailsPersistently(t *testing.T) {
	for _, tc := range []struct {
		name     string
		s        script
		requests int // model requests in all
		refunds  int
	}{
		{"400", script{failAfter: "refund_payment", failWith: "400"}, 8, 1},
		{"refusal", script{failAfter: "refund_payment", failWith: "refusal"}, 8, 1},
		{"max_tokens", script{failAfter: "refund_payment", failWith: "max_tokens"}, 8, 1},
		{"never_stops", script{loop: true}, 24, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useScript(t, tc.s)
			r := newRig(t)
			r.skipBackoff()
			r.start("a", "")
			r.send("a")
			r.check("FAILED", tc.refunds, 0)
			if got := r.query(`SELECT recovery_attempts::text FROM runs WHERE conversation_id = $1`); got != "6" {
				t.Errorf("recovery_attempts = %s, want 6: five recoveries, then the guard", got)
			}
			time.Sleep(time.Second) // two more reconciler passes: nothing more may be spent
			if got := r.modelRequests(); got != tc.requests {
				t.Errorf("%d model requests, want %d", got, tc.requests)
			}
			if h := r.history(); h[len(h)-1].GetToolResult() == nil {
				t.Errorf("history ends in %v; want the last tool result, nothing of the failed turn", h[len(h)-1])
			}
			if strings.Contains(r.ledger(), "send_email") {
				t.Errorf("ledger %s: a call from a failed turn was planned", r.ledger())
			}
		})
	}
}

// The real API may only be called with a Haiku model. The meter refuses any
// other model and stops the run. Checked against the mock, so nothing
// leaves the machine.
func TestMeterAllowsOnlyHaiku(t *testing.T) {
	mockOnly(t, "it must never reach the real API")
	up := httptest.NewServer(&mockModel{})
	defer up.Close()
	post := func(m *meter, model string) int {
		srv := httptest.NewServer(m)
		defer srv.Close()
		body := fmt.Sprintf(`{"model":%q,"max_tokens":16,"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`, model)
		resp, err := http.Post(srv.URL+"/v1/messages", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	guarded := newMeter(up.URL, 10, true)
	if got := post(guarded, "claude-haiku-5-5"); got != http.StatusOK {
		t.Errorf("Haiku request: %d, want 200", got)
	}
	if got := post(guarded, "claude-sonnet-5"); got != http.StatusBadRequest || guarded.aborted() == "" {
		t.Errorf("Sonnet request: %d, aborted %q; want 400 and an abort", got, guarded.aborted())
	}
	if got := post(guarded, "claude-haiku-5-5"); got != http.StatusBadRequest {
		t.Errorf("Haiku request after the abort: %d, want 400", got)
	}
	if got := guarded.count(); got != 1 {
		t.Errorf("%d requests sent on, want 1", got)
	}
	if got := post(newMeter(up.URL, 10, false), "claude-sonnet-5"); got != http.StatusOK {
		t.Errorf("unguarded meter: %d, want 200", got)
	}
}
