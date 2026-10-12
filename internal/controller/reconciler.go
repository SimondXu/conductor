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
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/telemetry"
	"github.com/simondxu/conductor/proto"
)

// ReconcilerConfig configures RunReconciler. Zero fields take the defaults
// of ARCHITECTURE.md §6.1.
type ReconcilerConfig struct {
	// Interval is the pause between passes, plus up to 20% jitter. 5s.
	Interval time.Duration
	// Concurrency bounds the runs recovered at once. 4.
	Concurrency int
	// MaxAttempts is how many recoveries a run gets before it is failed as
	// poisoned. 5.
	MaxAttempts int
}

// RunReconciler finds abandoned runs and drives them to a terminal state,
// until ctx ends (ARCHITECTURE.md §6.1). Each recovery is an Exec with no
// input, the same path a client's --resume takes, so the conversation's
// lease decides who runs it. It needs PostgreSQL: the runs index, the tool
// ledger and leases.
func (d *Controller) RunReconciler(ctx context.Context, cfg ReconcilerConfig) error {
	runs, ok := d.eventLog.(eventlog.RunIndex)
	if !ok || d.ledger == nil || d.lease == nil {
		return errors.New("the reconciler needs PostgreSQL: a runs index, a tool ledger and leases")
	}
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 4
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 5
	}
	for {
		if err := d.reconcileOnce(ctx, runs, cfg); err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "Reconciler pass failed", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(cfg.Interval + rand.N(cfg.Interval/5+1)):
		}
	}
}

// reconcileOnce is one pass: abandon calls past their deadline, then recover
// each candidate, cfg.Concurrency at a time.
func (d *Controller) reconcileOnce(ctx context.Context, runs eventlog.RunIndex, cfg ReconcilerConfig) error {
	if n, err := d.ledger.ExpireDeadlines(ctx); err != nil {
		return err
	} else if n > 0 {
		slog.InfoContext(ctx, "Abandoned tool calls past their reconcile_deadline", slog.Int("calls", n))
	}
	unsettled, err := d.ledger.CountUnsettled(ctx)
	if err != nil {
		return err
	}
	for _, s := range []toolledger.State{toolledger.Started, toolledger.OutcomeUnknown} {
		toolCallsUnsettled.WithLabelValues(string(s)).Set(float64(unsettled[s]))
	}
	cands, err := runs.RecoveryCandidates(ctx, d.registry.HarnessIDs(), 4*cfg.Concurrency)
	if err != nil {
		return err
	}
	sem := make(chan struct{}, cfg.Concurrency)
	var wg sync.WaitGroup
	for _, c := range cands {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			d.recoverOne(ctx, runs, c, cfg.MaxAttempts)
		}()
	}
	wg.Wait()
	return nil
}

// recoverOne recovers one conversation. The poison guard comes first, in its
// own committed transaction, so a run whose recovery crashes the process is
// still counted.
func (d *Controller) recoverOne(ctx context.Context, runs eventlog.RunIndex, c eventlog.Candidate, maxAttempts int) {
	id := c.ConversationID
	lctx := telemetry.WithLogAttrs(ctx, slog.String("conversation_id", id))
	if d.registry.Recovery(c.AgentID) != RecoveryFull {
		// The harness runs its own tool loop: Conductor cannot rebuild it
		// (correctness.md §0.3). Count it, then leave it alone for an hour.
		recoverySkipped.WithLabelValues("harness_not_recoverable").Inc()
		if _, err := runs.BumpRecoveryAttempt(ctx, id, func(int) time.Duration { return time.Hour }); err != nil {
			slog.WarnContext(lctx, "Skipped run not deferred", slog.Any("error", err))
		}
		return
	}
	n, err := runs.BumpRecoveryAttempt(ctx, id, recoveryBackoff)
	if err != nil {
		slog.WarnContext(lctx, "Recovery attempt not counted", slog.Any("error", err))
		return
	}
	if n > maxAttempts {
		if err := d.poison(lctx, id); err != nil && !errors.Is(err, lease.ErrHeld) {
			slog.WarnContext(lctx, "Failed to fail a poisoned run", slog.Any("error", err))
		}
		return
	}
	slog.InfoContext(lctx, "recovery.start", slog.Int("attempt", n),
		slog.String("kill_kind", c.KillKind), slog.Float64("idle_seconds", c.Idle.Seconds()))
	start := time.Now()
	err = d.Exec(ctx, &proto.CreateInteractionEvent{ConversationId: id}, nil)
	result := "success"
	switch {
	case err == nil:
		recoveryDuration.WithLabelValues(c.KillKind).Observe((c.Idle + time.Since(start)).Seconds())
	case errors.Is(err, lease.ErrHeld):
		result = "held"
	default:
		result = "failure"
	}
	recoveryTotal.WithLabelValues(result).Inc()
	slog.InfoContext(lctx, "recovery.complete", slog.String("result", result), slog.Int("attempt", n),
		slog.Float64("duration_seconds", time.Since(start).Seconds()), slog.Any("error", err))
}

// poison fails a PENDING run whose recovery keeps failing. Calls it left
// STARTED become OUTCOME_UNKNOWN, so their deadline still bounds them (I1).
func (d *Controller) poison(ctx context.Context, id string) error {
	ctx, gen, release, err := d.own(ctx, id)
	if err != nil {
		return err
	}
	defer release()
	events, err := d.eventLog.Events(ctx, id)
	if err != nil {
		return err
	}
	st, err := rebuild(events)
	if err != nil || st.State != proto.State_STATE_PENDING {
		return err
	}
	calls, err := d.ledger.Calls(ctx, id)
	if err != nil {
		return err
	}
	for _, c := range calls {
		if c.State == toolledger.Started {
			if err := d.ledger.MarkUnknown(ctx, gen, c.CallID, "recovery poisoned"); err != nil {
				return err
			}
		}
	}
	l := newLogger(d.eventLog, id, "")
	l.gen = gen
	if _, err := l.LogOutputs(ctx, nil, proto.State_STATE_FAILED); err != nil {
		return err
	}
	recoveryPoisoned.Inc()
	slog.WarnContext(ctx, "recovery.poisoned")
	return nil
}

// recoveryBackoff is the wait before recovery attempt n+1: 5s doubling, at
// most 5m.
func recoveryBackoff(n int) time.Duration {
	return min(5*time.Second<<min(n-1, 7), 5*time.Minute)
}
