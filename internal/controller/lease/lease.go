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

// Package lease is conversation ownership (docs/conductor/ARCHITECTURE.md
// §2.2, §5.1, §6.2): one owner per conversation at a time, with a generation
// that every committed write carries as its fencing token.
//
// The database clock alone decides ownership. A holder only decides,
// conservatively, when to stop: it turns the ttl the database returned into a
// local monotonic deadline anchored at the moment the request was sent. A
// database timestamp is never compared to time.Now().
package lease

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/simondxu/conductor/internal/chaos"
)

// Lease metrics (docs/conductor/ARCHITECTURE.md §7.2).
var (
	acquisitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_lease_acquisitions_total",
		Help: "Lease Acquire calls by result: acquired, held or error.",
	}, []string{"result"})
	losses = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "conductor_lease_losses_total",
		Help: "Leases lost while held, by reason: preempted, renew_timeout or deadline_elapsed.",
	}, []string{"reason"})
)

// Every label value exists from the start at 0, so increase() over a window
// sees a first event too, also in a replica that restarted inside it.
func init() {
	for _, r := range []string{"acquired", "held", "error"} {
		acquisitions.WithLabelValues(r)
	}
	for _, r := range []string{lossPreempted, lossRenewTimeout, lossDeadlineElapsed} {
		losses.WithLabelValues(r)
	}
}

var (
	// ErrHeld is returned by Acquire when an unexpired lease exists. A lease
	// is not reentrant: this includes one held by the same owner.
	ErrHeld = errors.New("lease: already held")
	// ErrUnsupported is returned by Acquire on a backend without fencing
	// (SQLite without --single-process).
	ErrUnsupported = errors.New("lease: unsupported on this backend")
	// ErrLost is returned by Renew and Release once the lease is no longer
	// ours: it expired, a higher generation took it, or the local deadline
	// elapsed.
	ErrLost = errors.New("lease: lost")
)

// Lease hands out ownership of conversations.
type Lease interface {
	// Acquire takes ownership, bumping the conversation's generation.
	// ErrHeld when an unexpired lease exists.
	Acquire(ctx context.Context, conversationID, ownerID string, ttl time.Duration) (Held, error)
}

// Held is one acquired lease.
type Held interface {
	// Generation is the fencing token for every write made under this lease.
	Generation() int64
	// Remaining is a local monotonic budget: the ttl the database last
	// returned, counted from when that request was sent. Zero once lost.
	Remaining() time.Duration
	// Renew extends the lease. It never waits past Remaining(), whatever
	// ctx allows. ErrLost when the database no longer has us as the owner;
	// any other error is transient and leaves the lease to the deadline.
	Renew(ctx context.Context) error
	// Release gives the lease up, preserving its generation, and closes Done.
	Release(ctx context.Context) error
	// Done closes when the lease is lost: the database refused a renewal
	// (expired, or a higher generation holds it), the local deadline elapsed
	// even with a renewal in flight, or Release was called. Execution must
	// stop.
	Done() <-chan struct{}
}

// KeepAlive renews h every period (ttl/3; ARCHITECTURE.md §10 uses a 30s ttl),
// each attempt bounded by period, until ctx ends or the lease is lost. Run it
// in its own goroutine and watch h.Done().
func KeepAlive(ctx context.Context, h Held, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.Done():
			return
		case <-t.C:
		}
		rctx, cancel := context.WithTimeout(ctx, period)
		err := h.Renew(rctx) // a transient failure is retried; the deadline bounds it
		cancel()
		if err == nil {
			chaos.At(ctx, chaos.LeaseRenewed, "")
		}
	}
}
