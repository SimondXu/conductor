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

package bench

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/simondxu/conductor/internal/invariant"
)

// Report is `make bench`'s output (docs/conductor/PRD.md §8). Durations in
// JSON are milliseconds.
type Report struct {
	Commit     string    `json:"commit"`
	Dirty      bool      `json:"dirty"`
	Started    time.Time `json:"started"`
	DurationMS int64     `json:"duration_ms"`
	Postgres   string    `json:"postgres"`
	Replicas   int       `json:"replicas"`
	Executor   string    `json:"executor"`
	Harness    string    `json:"harness"`

	LoadConfig LoadConfig  `json:"load_config"`
	Load       LoadSummary `json:"load"`

	Recovery  []ClassSummary `json:"recovery"`
	Total     ClassSummary   `json:"recovery_total"`
	Abandoned int            `json:"recovery_abandoned"`
	// Injections are every chaos run, for diffing beyond the summary.
	Injections []Injection `json:"injections"`

	Invariants []invariant.Result `json:"invariants"`
	// I8 is not checkable after the fact: the guard's decisions are counted
	// instead, and TestUnsafeToolNotStartedNearLeaseExpiry falsifies it.
	I8 struct {
		Allowed  int `json:"allowed"`
		Deferred int `json:"deferred"`
	} `json:"i8_unsafe_tool_turns"`

	Ledger    []LedgerRow `json:"tool_ledger"`
	Ownership Ownership   `json:"ownership"`
	// DuplicateEffects is I2's violation count.
	DuplicateEffects int      `json:"duplicate_effects"`
	EventLog         EventLog `json:"event_log"`

	Violations int    `json:"invariant_violations"`
	Result     string `json:"result"`
}

// LoadSummary is LoadResult with percentiles.
type LoadSummary struct {
	LoadResult
	ThroughputPerMin float64  `json:"throughput_sessions_per_min"`
	TurnMS           [3]int64 `json:"turn_latency_p50_p95_p99_ms"`
	RunMS            [3]int64 `json:"run_latency_p50_p95_p99_ms"`
	ErrorRatePct     float64  `json:"error_rate_pct"`
}

// ClassSummary is one RECOVERY row.
type ClassSummary struct {
	Class      string   `json:"class"`
	Injected   int      `json:"injected"`
	Recovered  int      `json:"recovered"`
	RatePct    float64  `json:"rate_pct"`
	RecoveryMS [3]int64 `json:"recovery_p50_p95_p99_ms"`
}

// LedgerRow is one TOOL LEDGER row. Unknown counts calls whose outcome was
// unknown at some point: retried (attempts > 1), abandoned or still unknown.
// Reconciled are those of them settled SUCCEEDED or FAILED.
type LedgerRow struct {
	Tool       string `json:"tool"`
	Class      string `json:"class"`
	Calls      int    `json:"calls"`
	Attempts   int    `json:"attempts"`
	Unknown    int    `json:"unknown"`
	Reconciled int    `json:"reconciled"`
	Abandoned  int    `json:"abandoned"`
}

// Ownership counters come from Prometheus, increase() over the bench, so a
// SIGKILLed replica's counts since its last scrape are lost. MaxGeneration
// is from the database.
type Ownership struct {
	Acquired       int            `json:"lease_acquired"`
	Contended      int            `json:"lease_contended"`
	Losses         map[string]int `json:"lease_losses"`
	FencedRejected int            `json:"fenced_writes_rejected"`
	MaxGeneration  int64          `json:"max_generation"`
}

// EventLog is from the database. A replay is counted for every takeover in
// a chaos conversation: the events below the new owner's generation.
type EventLog struct {
	Appended        int `json:"events_appended"`
	Replayed        int `json:"events_replayed_in_recovery"`
	ReplayPerRecP95 int `json:"replay_events_per_recovery_p95"`
	// LossDetected is I5's violation count: a gap in a conversation's steps.
	LossDetected int `json:"event_loss_detected"`
}

// nearestRank is the nearest-rank p50, p95 and p99 of xs; zero if empty.
func nearestRank[T cmp.Ordered](xs []T) [3]T {
	var out [3]T
	if len(xs) == 0 {
		return out
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	for i, p := range []float64{0.50, 0.95, 0.99} {
		out[i] = s[max(int(math.Ceil(p*float64(len(s))))-1, 0)]
	}
	return out
}

// percentiles is nearestRank in milliseconds.
func percentiles(ds []time.Duration) [3]int64 {
	p := nearestRank(ds)
	return [3]int64{p[0].Milliseconds(), p[1].Milliseconds(), p[2].Milliseconds()}
}

func summarizeLoad(r LoadResult) LoadSummary {
	s := LoadSummary{LoadResult: r, TurnMS: percentiles(r.TurnLatencies), RunMS: percentiles(r.RunLatencies)}
	if r.Elapsed > 0 {
		s.ThroughputPerMin = float64(r.Completed) / r.Elapsed.Minutes()
	}
	if r.Started > 0 {
		s.ErrorRatePct = 100 * float64(r.Failed) / float64(r.Started)
	}
	return s
}

func summarizeClass(class string, ins []Injection) ClassSummary {
	c := ClassSummary{Class: class, Injected: len(ins)}
	var times []time.Duration
	for _, in := range ins {
		if in.Recovered() {
			c.Recovered++
			times = append(times, in.RecoveryTime())
		}
	}
	if c.Injected > 0 {
		c.RatePct = 100 * float64(c.Recovered) / float64(c.Injected)
	}
	c.RecoveryMS = percentiles(times)
	return c
}

// drain waits until no run in scope is PENDING, so I1 is checked after the
// reconcilers have had their chance.
func (s *Stack) drain(ctx context.Context, scope string, timeout time.Duration) error {
	return waitFor(ctx, timeout, "runs in "+scope+" to end", func(ctx context.Context) (bool, error) {
		var n int
		err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM runs WHERE conversation_id LIKE $1 AND state = 'PENDING'", scope+"%").Scan(&n)
		return n == 0, err
	})
}

func (s *Stack) ledger(ctx context.Context, scope string) ([]LedgerRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT tool_name, class, count(*), COALESCE(sum(attempts), 0),
		count(*) FILTER (WHERE attempts > 1 OR state IN ('OUTCOME_UNKNOWN', 'ABANDONED')),
		count(*) FILTER (WHERE attempts > 1 AND state IN ('SUCCEEDED', 'FAILED')),
		count(*) FILTER (WHERE state = 'ABANDONED')
		FROM tool_calls WHERE conversation_id LIKE $1 GROUP BY 1, 2 ORDER BY 2, 1`, scope+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LedgerRow
	for rows.Next() {
		var r LedgerRow
		if err := rows.Scan(&r.Tool, &r.Class, &r.Calls, &r.Attempts, &r.Unknown, &r.Reconciled, &r.Abandoned); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Stack) eventLog(ctx context.Context, scope, chaosScope string) (EventLog, error) {
	var e EventLog
	if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM conversation_log WHERE conversation_id LIKE $1", scope+"%").Scan(&e.Appended); err != nil {
		return e, err
	}
	rows, err := s.db.QueryContext(ctx, `
		WITH g AS (SELECT DISTINCT conversation_id, generation FROM conversation_log
		            WHERE conversation_id LIKE $1 AND generation IS NOT NULL),
		     t AS (SELECT conversation_id, generation FROM g
		            WHERE generation > (SELECT min(generation) FROM g g2 WHERE g2.conversation_id = g.conversation_id))
		SELECT (SELECT count(*) FROM conversation_log l
		         WHERE l.conversation_id = t.conversation_id AND l.generation < t.generation) FROM t`, chaosScope+"%")
	if err != nil {
		return e, err
	}
	defer rows.Close()
	var per []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			return e, err
		}
		e.Replayed += n
		per = append(per, n)
	}
	e.ReplayPerRecP95 = nearestRank(per)[1]
	return e, rows.Err()
}

// prometheus queries sum by (label) (increase(metric[window])), rounded.
// With label "" it returns the plain sum under "".
func prometheus(ctx context.Context, base, metric, label string, window time.Duration) (map[string]int, error) {
	by := ""
	if label != "" {
		by = " by (" + label + ")"
	}
	q := fmt.Sprintf("sum%s (increase(%s[%ds]))", by, metric, int(window.Seconds()))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/query?query="+url.QueryEscape(q), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Status string
		Data   struct {
			Result []struct {
				Metric map[string]string
				Value  [2]any
			}
		}
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Status != "success" {
		return nil, fmt.Errorf("prometheus %s: %s", q, body)
	}
	m := map[string]int{}
	for _, r := range out.Data.Result {
		v, _ := strconv.ParseFloat(fmt.Sprint(r.Value[1]), 64)
		m[r.Metric[label]] = int(math.Round(v))
	}
	return m, nil
}
