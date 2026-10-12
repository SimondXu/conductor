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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Run metrics (docs/conductor/ARCHITECTURE.md §7.2). A run is one Exec call.
var (
	runsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "conductor_runs_active",
		Help: "Exec calls in progress on this replica, by owner id.",
	}, []string{"owner"})
	runsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_runs_total",
		Help: "Finished Exec calls by outcome.",
	}, []string{"outcome"})
	runDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "conductor_run_duration_seconds",
		Help:    "Exec call latency by outcome.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14), // 10ms .. ~82s
	}, []string{"outcome"})
)

// Recovery metrics (ARCHITECTURE.md §7.2).
var (
	recoveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_recovery_total",
		Help: "Reconciler recovery attempts by result: success, failure or held (another owner won).",
	}, []string{"result"})
	recoveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "conductor_recovery_duration_seconds",
		Help: "How long a recovered run was stalled: from its last append before recovery to " +
			"the end of the recovery, by how its last owner let go (graceful, ungraceful, none).",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s .. ~17m
	}, []string{"kill_kind"})
	recoverySkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_recovery_skipped_total",
		Help: "Abandoned runs the reconciler cannot recover, by reason; each is counted at most once an hour.",
	}, []string{"reason"})
	recoveryPoisoned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "conductor_recovery_poisoned_total",
		Help: "Runs failed because their recovery kept failing.",
	})
	toolCallsUnsettled = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "conductor_tool_calls_unsettled",
		Help: "Tool calls in STARTED or OUTCOME_UNKNOWN across the database, sampled each reconciler pass; " +
			"every replica reports the same value, so aggregate with max.",
	}, []string{"state"})
	replayEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "conductor_replay_events_total",
		Help: "Events read to rebuild conversations, one full replay per Exec.",
	})
)

func runOutcome(err error) string {
	switch {
	case err == nil:
		return "completed"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "failed"
	}
}
