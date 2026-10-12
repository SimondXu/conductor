// Copyright 2026 Google LLC
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

package eventlog

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	gproto "google.golang.org/protobuf/proto"

	schema "github.com/simondxu/conductor/db"
	"github.com/simondxu/conductor/proto"
)

// testEventLog runs the EventLog contract against a backend. newLog returns a
// fresh, empty event log for each subtest, using the subtest's *testing.T for
// temp dirs and cleanup.
//
// Subtests derive their IDs from t.Name() plus a random suffix, so reruns
// against a shared Postgres database never see an earlier run's rows.
func testEventLog(t *testing.T, newLog func(t *testing.T) EventLog) {
	t.Run("AppendAndEvents", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		conv := t.Name() + "-conv-1-" + uuid.NewString()
		task1 := t.Name() + "-task-1"
		task2 := t.Name() + "-task-2"

		// 1. Conversation log.
		cev1 := &proto.StepEvent{ConversationId: conv, InteractionId: task1}
		cev2 := &proto.StepEvent{ConversationId: conv, InteractionId: task2}
		s1, err := log.Append(ctx, cev1, 0)
		if err != nil {
			t.Fatalf("failed to append cev1: %v", err)
		}
		s2, err := log.Append(ctx, cev2, 0)
		if err != nil {
			t.Fatalf("failed to append cev2: %v", err)
		}

		cEvents, err := log.Events(ctx, conv)
		if err != nil {
			t.Fatalf("failed to read conversation events: %v", err)
		}
		if len(cEvents) != 2 {
			t.Fatalf("expected 2 conversation events, got %d", len(cEvents))
		}
		if s1 != 1 || s2 != 2 {
			t.Errorf("conversation events out of order: %d, %d", s1, s2)
		}
		if cEvents[0].InteractionId != task1 || cEvents[1].InteractionId != task2 {
			t.Errorf("conversation events mismatch: %q, %q", cEvents[0].InteractionId, cEvents[1].InteractionId)
		}

	})

	// I10: a retried append with the same event_id is a no-op that returns
	// the existing step.
	t.Run("AppendIdempotentUnderRetry", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)
		conv := t.Name() + "-" + uuid.NewString()

		ev := &proto.StepEvent{ConversationId: conv, EventId: uuid.NewString(), Steps: []*proto.Step{{Description: "x"}}}
		first, err := log.Append(ctx, ev, 0)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := log.Append(ctx, gproto.Clone(ev).(*proto.StepEvent), 0)
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		if retry != first {
			t.Errorf("retry returned step %d, want %d", retry, first)
		}
		next, err := log.Append(ctx, &proto.StepEvent{ConversationId: conv, EventId: uuid.NewString()}, 0)
		if err != nil || next != first+1 {
			t.Errorf("next append = %d, %v; want %d", next, err, first+1)
		}
		events, _ := log.Events(ctx, conv)
		if len(events) != 2 {
			t.Errorf("events = %d, want 2", len(events))
		}
	})

	t.Run("Empty", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		events, err := log.Events(ctx, t.Name()+"-none")
		if err != nil {
			t.Fatalf("failed to read events: %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("expected 0 events, got %d", len(events))
		}
	})

	// AutoStep exercises the step==0 auto-assignment path: appends with Step unset
	// receive sequential numbers starting at 1.
	t.Run("AutoStep", func(t *testing.T) {
		ctx := context.Background()
		log := newLog(t)

		conv := t.Name() + "-conv-" + uuid.NewString()

		const n = 3
		for i := int64(1); i <= n; i++ {
			step, err := log.Append(ctx, &proto.StepEvent{ConversationId: conv, InteractionId: "t"}, 0)
			if err != nil {
				t.Fatalf("auto-step append failed: %v", err)
			}
			if step != i {
				t.Errorf("expected step %d, got %d", i, step)
			}
		}

		events, err := log.Events(ctx, conv)
		if err != nil {
			t.Fatalf("failed to read events: %v", err)
		}
		if len(events) != n {
			t.Fatalf("expected %d events, got %d", n, len(events))
		}
	})
}

// TestSQLiteEventLog runs the EventLog contract against the SQLite backend.
func TestSQLiteEventLog(t *testing.T) {
	testEventLog(t, func(t *testing.T) EventLog {
		t.Helper()
		log, err := OpenSQLiteEventLog(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("failed to open sqlite event log: %v", err)
		}
		t.Cleanup(func() { log.Close() })
		return log
	})
}

// TestPostgresEventLog runs the EventLog contract against the Postgres backend
// described by PG_TEST_DSN, skipping when that variable is not set.
func TestPostgresEventLog(t *testing.T) {
	dsn := os.Getenv("PG_TEST_DSN")
	if dsn == "" {
		t.Skip("PG_TEST_DSN not set; skipping Postgres event log tests")
	}
	if _, err := schema.Up(dsn); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	testEventLog(t, func(t *testing.T) EventLog {
		t.Helper()
		log, err := OpenPostgresEventLog(dsn)
		if err != nil {
			t.Fatalf("failed to open postgres event log: %v", err)
		}
		t.Cleanup(func() { log.Close() })
		return log
	})
}

// TestSQLiteEventLog_CreatesParentDirectory is SQLite-specific: OpenSQLiteEventLog
// must create the database file's parent directory if it does not exist.
func TestSQLiteEventLog_CreatesParentDirectory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "newdir", "test.db")

	log, err := OpenSQLiteEventLog(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite event log and create directory: %v", err)
	}
	defer log.Close()

	if _, err := os.Stat(filepath.Dir(dbPath)); os.IsNotExist(err) {
		t.Fatalf("expected parent directory to be created, but it does not exist")
	}
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatalf("expected database file to be created, but it does not exist")
	}
}

// A SQLite file created before event_id existed is upgraded on open.
func TestSQLiteEventLog_UpgradesOldSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE conversation_log (conversation_id TEXT NOT NULL, step INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (conversation_id, step))`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	log, err := OpenSQLiteEventLog(path)
	if err != nil {
		t.Fatalf("open old schema: %v", err)
	}
	defer log.Close()
	ev := &proto.StepEvent{ConversationId: "c", EventId: uuid.NewString()}
	a, err := log.Append(context.Background(), ev, 0)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := log.Append(context.Background(), ev, 0); err != nil || b != a {
		t.Fatalf("retry = %d, %v; want %d", b, err, a)
	}
}
