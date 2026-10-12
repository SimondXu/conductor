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
	"errors"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	schema "github.com/simondxu/conductor/db"
)

// openTestLease returns a lease on PG_TEST_DSN, skipping when it is unset. The
// test's conversation is t.Name(); its row is cleared first.
func openTestLease(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping Postgres lease tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	p, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if _, err := p.db.Exec("DELETE FROM conversation_leases WHERE conversation_id = $1", t.Name()); err != nil {
		t.Fatal(err)
	}
	return p
}

func acquire(t *testing.T, p *Postgres, owner string, ttl time.Duration) Held {
	t.Helper()
	h, err := p.Acquire(context.Background(), t.Name(), owner, ttl)
	if err != nil {
		t.Fatalf("acquire by %s: %v", owner, err)
	}
	return h
}

func waitDone(t *testing.T, h Held, within time.Duration) {
	t.Helper()
	select {
	case <-h.Done():
	case <-time.After(within):
		t.Fatalf("Done did not close within %v", within)
	}
}

func isDone(h Held) bool {
	select {
	case <-h.Done():
		return true
	default:
		return false
	}
}

func TestAcquireWhileHeld(t *testing.T) {
	p := openTestLease(t)
	ctx := context.Background()
	a := acquire(t, p, "a", 30*time.Second)
	if a.Generation() != 1 {
		t.Fatalf("first generation = %d, want 1", a.Generation())
	}
	if r := a.Remaining(); r <= 0 || r > 30*time.Second {
		t.Fatalf("remaining = %v, want (0, 30s]", r)
	}
	for _, owner := range []string{"b", "a"} {
		if _, err := p.Acquire(ctx, t.Name(), owner, 30*time.Second); !errors.Is(err, ErrHeld) {
			t.Errorf("acquire by %s: err = %v, want ErrHeld", owner, err)
		}
	}
	if err := a.Renew(ctx); err != nil {
		t.Fatalf("renew by holder: %v", err)
	}
}

func TestTakeoverAfterExpiry(t *testing.T) {
	p := openTestLease(t)
	ctx := context.Background()
	a := acquire(t, p, "a", 300*time.Millisecond)
	waitDone(t, a, 2*time.Second)

	// The local deadline is anchored at send time, so it fires no later than
	// the database's expiry; retry briefly until the database agrees.
	var b Held
	var err error
	for start := time.Now(); time.Since(start) < 2*time.Second; time.Sleep(20 * time.Millisecond) {
		if b, err = p.Acquire(ctx, t.Name(), "b", 30*time.Second); !errors.Is(err, ErrHeld) {
			break
		}
	}
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if b.Generation() != a.Generation()+1 {
		t.Fatalf("takeover generation = %d, want %d", b.Generation(), a.Generation()+1)
	}
	if err := a.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("renew after expiry: err = %v, want ErrLost", err)
	}
}

func TestReleasePreservesGeneration(t *testing.T) {
	p := openTestLease(t)
	ctx := context.Background()
	a := acquire(t, p, "a", 30*time.Second)
	if err := a.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if !isDone(a) || a.Remaining() != 0 {
		t.Fatal("release did not end the lease locally")
	}
	var owner *string
	var gen int64
	if err := p.db.QueryRow("SELECT owner_id, generation FROM conversation_leases WHERE conversation_id = $1",
		t.Name()).Scan(&owner, &gen); err != nil {
		t.Fatalf("released row: %v", err)
	}
	if owner != nil || gen != a.Generation() {
		t.Fatalf("released row: owner %v generation %d, want NULL and %d", owner, gen, a.Generation())
	}

	b := acquire(t, p, "b", 30*time.Second)
	if b.Generation() != a.Generation()+1 {
		t.Fatalf("generation after release = %d, want %d", b.Generation(), a.Generation()+1)
	}
	if err := a.Release(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("second release: err = %v, want ErrLost", err)
	}
}

func TestRenewWithStaleGenerationFails(t *testing.T) {
	p := openTestLease(t)
	ctx := context.Background()
	a := acquire(t, p, "a", 30*time.Second)
	// Expire a in the database while its local deadline is still far off,
	// as a GC pause or a slow clock would, and let b take over.
	if _, err := p.db.Exec("UPDATE conversation_leases SET expires_at = now() WHERE conversation_id = $1", t.Name()); err != nil {
		t.Fatal(err)
	}
	b := acquire(t, p, "b", 30*time.Second)
	if b.Generation() != a.Generation()+1 {
		t.Fatalf("takeover generation = %d, want %d", b.Generation(), a.Generation()+1)
	}
	if isDone(a) {
		t.Fatal("a lost its lease before observing the takeover")
	}

	preempted := testutil.ToFloat64(losses.WithLabelValues(lossPreempted))
	if err := a.Renew(ctx); !errors.Is(err, ErrLost) {
		t.Fatalf("stale renew: err = %v, want ErrLost", err)
	}
	if got := testutil.ToFloat64(losses.WithLabelValues(lossPreempted)) - preempted; got != 1 {
		t.Errorf("preempted losses += %v, want 1", got)
	}
	if !isDone(a) {
		t.Fatal("Done not closed after a refused renewal")
	}
	if err := b.Renew(ctx); err != nil || isDone(b) {
		t.Fatalf("current holder renew: err = %v, done = %t", err, isDone(b))
	}
}

func TestDeadlineFiresWhileRenewIsStuck(t *testing.T) {
	p := openTestLease(t)
	const ttl = time.Second
	a := acquire(t, p, "a", ttl)

	// Hold the row lock so the renewal's UPDATE blocks in the database.
	tx, err := p.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("SELECT 1 FROM conversation_leases WHERE conversation_id = $1 FOR UPDATE", t.Name()); err != nil {
		t.Fatal(err)
	}

	timeouts := testutil.ToFloat64(losses.WithLabelValues(lossRenewTimeout))
	renewed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		renewed <- a.Renew(ctx)
	}()

	start := time.Now()
	waitDone(t, a, ttl+500*time.Millisecond)
	if waited := time.Since(start); waited > ttl+200*time.Millisecond {
		t.Fatalf("Done closed after %v, past the %v deadline", waited, ttl)
	}
	if a.Remaining() != 0 {
		t.Fatalf("remaining = %v after the deadline", a.Remaining())
	}
	if got := testutil.ToFloat64(losses.WithLabelValues(lossRenewTimeout)) - timeouts; got != 1 {
		t.Errorf("renew_timeout losses += %v, want 1", got)
	}
	select {
	case err := <-renewed:
		if err == nil {
			t.Fatal("stuck renewal reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stuck renewal outlived the local deadline")
	}
}

func TestKeepAliveHoldsPastTTL(t *testing.T) {
	p := openTestLease(t)
	const ttl = 600 * time.Millisecond
	a := acquire(t, p, "a", ttl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go KeepAlive(ctx, a, ttl/3)

	time.Sleep(3 * ttl)
	if isDone(a) || a.Remaining() <= 0 {
		t.Fatalf("lease lost under KeepAlive: remaining %v", a.Remaining())
	}
	if _, err := p.Acquire(context.Background(), t.Name(), "b", ttl); !errors.Is(err, ErrHeld) {
		t.Fatalf("acquire by b: err = %v, want ErrHeld", err)
	}
}
