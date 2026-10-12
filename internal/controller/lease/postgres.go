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

package lease

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Postgres is the Lease on the conversation_leases table (migration 0004).
type Postgres struct {
	db *sql.DB
}

var _ Lease = (*Postgres)(nil)

// OpenPostgres connects to dsn. The schema is owned by `conductor migrate`.
func OpenPostgres(dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("lease: open: %w", err)
	}
	return &Postgres{db: db}, nil
}

// Close closes the connection pool.
func (p *Postgres) Close() error { return p.db.Close() }

// Acquire is one statement and uses only the database clock (§2.2). A row
// whose expires_at is not in the future is free; that includes a released
// one, whose generation the takeover increments.
func (p *Postgres) Acquire(ctx context.Context, conversationID, ownerID string, ttl time.Duration) (Held, error) {
	sent := time.Now()
	var gen int64
	var remaining float64
	err := p.db.QueryRowContext(ctx, `
		INSERT INTO conversation_leases AS l
		       (conversation_id, owner_id, generation, acquired_at, expires_at)
		VALUES ($1, $2, 1, now(), now() + make_interval(secs => $3))
		ON CONFLICT (conversation_id) DO UPDATE
		   SET owner_id    = $2,
		       generation  = l.generation + 1,
		       acquired_at = now(),
		       expires_at  = now() + make_interval(secs => $3)
		 WHERE l.expires_at <= now()
		RETURNING generation, EXTRACT(EPOCH FROM expires_at - now())::float8`,
		conversationID, ownerID, ttl.Seconds()).Scan(&gen, &remaining)
	if errors.Is(err, sql.ErrNoRows) {
		acquisitions.WithLabelValues("held").Inc()
		return nil, fmt.Errorf("%w: %s", ErrHeld, conversationID)
	}
	if err != nil {
		acquisitions.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("lease: acquire %s: %w", conversationID, err)
	}
	acquisitions.WithLabelValues("acquired").Inc()
	h := &held{
		p: p, conversationID: conversationID, ownerID: ownerID, gen: gen, ttl: ttl,
		done:     make(chan struct{}),
		deadline: localDeadline(sent, remaining),
	}
	h.mu.Lock() // expire may fire before the assignment on a tiny ttl
	h.timer = time.AfterFunc(time.Until(h.deadline), h.expire)
	h.mu.Unlock()
	return h, nil
}

// localDeadline anchors the database's remaining ttl at the moment the request
// was sent, so it can only be earlier than the real expiry.
func localDeadline(sent time.Time, remainingSecs float64) time.Time {
	return sent.Add(time.Duration(remainingSecs * float64(time.Second)))
}

type held struct {
	p              *Postgres
	conversationID string
	ownerID        string
	gen            int64
	ttl            time.Duration
	done           chan struct{}

	mu       sync.Mutex
	deadline time.Time // carries a monotonic reading
	timer    *time.Timer
	lost     bool
	// renewing and renewFailed tell a deadline that passed during, or after,
	// failed renewals from one no renewal tried to prevent.
	renewing, renewFailed bool
}

func (h *held) Generation() int64     { return h.gen }
func (h *held) Done() <-chan struct{} { return h.done }

func (h *held) Remaining() time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lost {
		return 0
	}
	return max(time.Until(h.deadline), 0)
}

// Renew is bounded by the local deadline, so a stalled round-trip cannot
// outlive the lease. GREATEST keeps expires_at from ever moving backwards when
// two renewals commit out of order, so every ttl it returns is a safe lower
// bound.
func (h *held) Renew(ctx context.Context) error {
	h.mu.Lock()
	lost, deadline := h.lost, h.deadline
	h.renewing = true
	h.mu.Unlock()
	if lost {
		return fmt.Errorf("%w: %s generation %d", ErrLost, h.conversationID, h.gen)
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	sent := time.Now()
	var remaining float64
	err := h.p.db.QueryRowContext(ctx, `
		UPDATE conversation_leases
		   SET expires_at = GREATEST(expires_at, now() + make_interval(secs => $4))
		 WHERE conversation_id = $1 AND owner_id = $2 AND generation = $3
		   AND expires_at > now()
		RETURNING EXTRACT(EPOCH FROM expires_at - now())::float8`,
		h.conversationID, h.ownerID, h.gen, h.ttl.Seconds()).Scan(&remaining)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.renewing = false
	if errors.Is(err, sql.ErrNoRows) {
		h.loseLocked(lossPreempted)
		return fmt.Errorf("%w: %s generation %d", ErrLost, h.conversationID, h.gen)
	}
	h.renewFailed = err != nil
	if err != nil {
		return fmt.Errorf("lease: renew %s: %w", h.conversationID, err)
	}
	if h.lost {
		return fmt.Errorf("%w: %s generation %d", ErrLost, h.conversationID, h.gen)
	}
	if d := localDeadline(sent, remaining); d.After(h.deadline) {
		h.deadline = d
		h.timer.Reset(time.Until(d))
	}
	return nil
}

// Release stops local work first, then frees the row. It never deletes it:
// the generation must outlive the lease (§2.2).
func (h *held) Release(ctx context.Context) error {
	h.mu.Lock()
	h.loseLocked("")
	h.mu.Unlock()
	res, err := h.p.db.ExecContext(ctx, `
		UPDATE conversation_leases SET owner_id = NULL, expires_at = now()
		 WHERE conversation_id = $1 AND owner_id = $2 AND generation = $3`,
		h.conversationID, h.ownerID, h.gen)
	if err != nil {
		return fmt.Errorf("lease: release %s: %w", h.conversationID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("lease: release %s: %w", h.conversationID, err)
	} else if n == 0 {
		return fmt.Errorf("%w: %s generation %d", ErrLost, h.conversationID, h.gen)
	}
	return nil
}

// expire runs on the timer goroutine, independent of any renewal in flight.
func (h *held) expire() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lost {
		return
	}
	if left := time.Until(h.deadline); left > 0 {
		h.timer.Reset(left) // a renewal extended the deadline as we fired
		return
	}
	if h.renewing || h.renewFailed {
		h.loseLocked(lossRenewTimeout)
	} else {
		h.loseLocked(lossDeadlineElapsed)
	}
}

// Loss reasons for conductor_lease_losses_total.
const (
	lossPreempted       = "preempted"        // the database no longer has us as owner
	lossRenewTimeout    = "renew_timeout"    // a renewal was stuck or failing at the deadline
	lossDeadlineElapsed = "deadline_elapsed" // the deadline passed with no renewal attempted in time
)

// loseLocked stops the lease. reason is empty for a Release, which is not a
// loss.
func (h *held) loseLocked(reason string) {
	if h.lost {
		return
	}
	if reason != "" {
		losses.WithLabelValues(reason).Inc()
	}
	h.lost = true
	h.timer.Stop()
	close(h.done)
}
