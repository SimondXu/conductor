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
	runsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "conductor_runs_active",
		Help: "Exec calls in progress on this replica.",
	})
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
