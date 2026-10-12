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
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/simondxu/conductor/internal/invariant"
)

// Config is one `conductor bench`.
type Config struct {
	Load    LoadConfig
	Classes []string
	// Repeat is the injections per class.
	Repeat int
	// Prometheus is the stack's Prometheus base URL.
	Prometheus string
	// Progress gets a line per phase.
	Progress io.Writer
}

// drainTimeout bounds the wait for every run to end before the checks.
const drainTimeout = 2 * time.Minute

// Bench runs the load, then every class Repeat times, waits for every run
// to end, and checks the invariants over all of it.
func (s *Stack) Bench(ctx context.Context, cfg Config) (*Report, error) {
	started := time.Now()
	scope := fmt.Sprintf("bench-%d-", started.Unix())
	r := &Report{Started: started, Replicas: len(s.replicas), Harness: "fake (deterministic)",
		Executor: "none: fixed harness endpoint (compose)"}
	r.Commit, r.Dirty = gitState()
	if err := s.db.QueryRowContext(ctx, "SHOW server_version").Scan(&r.Postgres); err != nil {
		return nil, err
	}

	cfg.Load.Prefix = scope + "load"
	r.LoadConfig = cfg.Load
	fmt.Fprintf(cfg.Progress, "load: %d sessions at a time for %s\n", cfg.Load.Concurrency, cfg.Load.Duration)
	r.Load = summarizeLoad(s.Load(ctx, cfg.Load))

	chaosScope := scope + "chaos-"
	for _, class := range cfg.Classes {
		fmt.Fprintf(cfg.Progress, "chaos: %s x%d\n", class, cfg.Repeat)
		var ins []Injection
		err := s.Chaos(ctx, class, "", cfg.Repeat, chaosScope+class, func(_ string, got []Injection) {
			ins = append(ins, got...)
		})
		if err != nil {
			return nil, err
		}
		r.Injections = append(r.Injections, ins...)
		r.Recovery = append(r.Recovery, summarizeClass(class, ins))
	}
	r.Total = summarizeClass("TOTAL", r.Injections)

	fmt.Fprintln(cfg.Progress, "drain and check")
	if err := s.drain(ctx, scope, drainTimeout); err != nil {
		fmt.Fprintf(cfg.Progress, "drain: %v (checking anyway)\n", err)
	}
	var err error
	if r.Invariants, err = invariant.Check(ctx, s.db, scope); err != nil {
		return nil, err
	}
	for _, res := range r.Invariants {
		r.Violations += res.Violations
		switch res.ID {
		case "I2":
			r.DuplicateEffects = res.Violations
		case "I5":
			r.EventLog.LossDetected = res.Violations
		}
	}
	if r.Ledger, err = s.ledger(ctx, scope); err != nil {
		return nil, err
	}
	appended, err := s.eventLog(ctx, scope, chaosScope)
	if err != nil {
		return nil, err
	}
	appended.LossDetected = r.EventLog.LossDetected
	r.EventLog = appended
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(max(generation), 0) FROM conversation_leases WHERE conversation_id LIKE $1", scope+"%").Scan(&r.Ownership.MaxGeneration); err != nil {
		return nil, err
	}

	// One more scrape, so the counters include the end of the run.
	time.Sleep(6 * time.Second)
	window := time.Since(started) + 10*time.Second
	prom := func(metric, label string) map[string]int {
		m, err := prometheus(ctx, cfg.Prometheus, metric, label, window)
		if err != nil {
			fmt.Fprintf(cfg.Progress, "prometheus: %v\n", err)
		}
		return m
	}
	acq := prom("conductor_lease_acquisitions_total", "result")
	r.Ownership.Acquired, r.Ownership.Contended = acq["acquired"], acq["held"]
	r.Ownership.Losses = prom("conductor_lease_losses_total", "reason")
	r.Ownership.FencedRejected = prom("conductor_fenced_writes_rejected_total", "")[""]
	r.Abandoned = prom("conductor_recovery_poisoned_total", "")[""]
	unsafe := prom("conductor_unsafe_tool_turns_total", "decision")
	r.I8.Allowed, r.I8.Deferred = unsafe["allowed"], unsafe["deferred"]

	r.DurationMS = time.Since(started).Milliseconds()
	r.Result = "PASS"
	if r.Violations > 0 || r.DuplicateEffects > 0 {
		r.Result = "FAIL"
	}
	return r, nil
}

// gitState is the working tree's commit and whether it has changes.
func gitState() (string, bool) {
	sha, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown", false
	}
	status, _ := exec.Command("git", "status", "--porcelain").Output()
	return strings.TrimSpace(string(sha)), len(strings.TrimSpace(string(status))) > 0
}

// Render writes the report as PRD.md §8.2's table.
func (r *Report) Render(w io.Writer) {
	p := func(format string, a ...any) { fmt.Fprintf(w, format+"\n", a...) }
	yesNo := map[bool]string{true: "yes", false: "no"}
	rule := strings.Repeat("-", 80)
	p("Conductor benchmark")
	p("  commit        %s   dirty: %s", r.Commit, yesNo[r.Dirty])
	p("  started       %s", r.Started.Format(time.RFC3339))
	p("  duration      %s", (time.Duration(r.DurationMS) * time.Millisecond).Round(time.Second))
	p("  backend       postgres %s", r.Postgres)
	p("  replicas      %d", r.Replicas)
	p("  executor      %s", r.Executor)
	p("  harness       %s", r.Harness)
	p("  load          %d concurrent sessions, %d turns/session, %d tool calls/turn",
		r.LoadConfig.Concurrency, r.LoadConfig.Turns, r.LoadConfig.ToolCalls)
	p("")
	l := r.Load
	p("LOAD")
	p("  %-40s %s", "metric", "value")
	p("  %s", strings.Repeat("-", 48))
	p("  %-40s %d", "sessions started", l.Started)
	p("  %-40s %d", "sessions completed", l.Completed)
	p("  %-40s %d", "sessions failed", l.Failed)
	p("  %-40s %d", "max concurrent sessions sustained", l.MaxConcurrent)
	p("  %-40s %.1f sessions/min", "throughput", l.ThroughputPerMin)
	p("  %-40s %d / %d / %d ms", "turn latency p50 / p95 / p99", l.TurnMS[0], l.TurnMS[1], l.TurnMS[2])
	p("  %-40s %d / %d / %d ms", "run latency  p50 / p95 / p99", l.RunMS[0], l.RunMS[1], l.RunMS[2])
	p("  %-40s %.2f %%", "error rate", l.ErrorRatePct)
	p("")
	p("RECOVERY  (by failure class)")
	row := func(c ClassSummary) {
		p("  %-27s %-6d %-11d %-8s %-8s %-8s %s", c.Class, c.Injected, c.Recovered, fmt.Sprintf("%.1f%%", c.RatePct),
			fmt.Sprintf("%dms", c.RecoveryMS[0]), fmt.Sprintf("%dms", c.RecoveryMS[1]), fmt.Sprintf("%dms", c.RecoveryMS[2]))
	}
	p("  %-27s %-6s %-11s %-8s %-8s %-8s %s", "class", "inj.", "recovered", "rate", "p50", "p95", "p99")
	p("  %s", rule)
	for _, c := range r.Recovery {
		row(c)
	}
	p("  %s", rule)
	row(r.Total)
	p("")
	p("  recovery abandoned (attempt cap reached)  %d", r.Abandoned)
	p("")
	p("INVARIANTS")
	p("  %-5s %-56s %-8s %s", "id", "assertion", "checks", "viol.")
	p("  %s", rule)
	checks := 0
	for _, res := range r.Invariants {
		checks += res.Checks
		p("  %-5s %-56s %-8d %d", res.ID, res.Assertion, res.Checks, res.Violations)
		if res.Violations > 0 {
			p("        e.g. %s", strings.Join(res.Examples, ", "))
		}
	}
	p("  %-5s %-56s %-8d %s", "I8", "no Conductor-owned UNSAFE start inside lease margin", r.I8.Allowed+r.I8.Deferred, "n/a")
	p("        guard decisions: %d allowed, %d deferred; falsified by TestUnsafeToolNotStartedNearLeaseExpiry", r.I8.Allowed, r.I8.Deferred)
	p("  %s", rule)
	p("  %-5s %-56s %-8d %d", "TOTAL", "", checks, r.Violations)
	p("")
	p("TOOL LEDGER")
	p("  %-15s %-14s %-6s %-9s %-8s %-11s %s", "tool", "class", "calls", "attempts", "unknown", "reconciled", "abandoned")
	p("  %s", rule)
	for _, t := range r.Ledger {
		p("  %-15s %-14s %-6d %-9d %-8d %-11d %d", t.Tool, t.Class, t.Calls, t.Attempts, t.Unknown, t.Reconciled, t.Abandoned)
	}
	p("")
	o := r.Ownership
	p("OWNERSHIP  (counters: Prometheus increase() over the run)")
	p("  lease acquisitions (success / contended)   %d / %d", o.Acquired, o.Contended)
	p("  lease losses by reason                     renew_timeout=%d deadline=%d preempted=%d",
		o.Losses["renew_timeout"], o.Losses["deadline_elapsed"], o.Losses["preempted"])
	p("  fenced writes rejected                     %d", o.FencedRejected)
	p("  max generation observed                    %d", o.MaxGeneration)
	p("")
	p("DUPLICATE EFFECTS")
	p("  IDEMPOTENT tools, extra effects per idempotency_key   %d", r.DuplicateEffects)
	p("  (I2's query over this run's conversations)")
	p("")
	e := r.EventLog
	p("EVENT LOG")
	p("  events appended                            %d", e.Appended)
	p("  events replayed during recovery            %d", e.Replayed)
	p("  replay events per recovery p95             %d", e.ReplayPerRecP95)
	p("  event loss detected                        %d", e.LossDetected)
	p("")
	p("RESULT: %s   (%d invariant violations, %d duplicate effects)", r.Result, r.Violations, r.DuplicateEffects)
}
