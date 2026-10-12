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

package fake

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Effect is one external side effect a fake tool performed.
type Effect struct {
	CallID         string
	IdempotencyKey string
	ToolName       string
	Payload        []byte // JSON arguments
}

// EffectSink records effects. Record returns how many effects now exist for
// the effect's idempotency key, including this one; Count returns how many
// exist already.
type EffectSink interface {
	Record(ctx context.Context, e Effect) (attempt int, err error)
	Count(ctx context.Context, idempotencyKey string) (int, error)
}

// MemoryEffects is an in-process EffectSink.
type MemoryEffects struct {
	mu      sync.Mutex
	Effects []Effect
}

// Record implements EffectSink.
func (m *MemoryEffects) Record(_ context.Context, e Effect) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Effects = append(m.Effects, e)
	n := 0
	for _, x := range m.Effects {
		if x.IdempotencyKey == e.IdempotencyKey {
			n++
		}
	}
	return n, nil
}

// Count implements EffectSink.
func (m *MemoryEffects) Count(_ context.Context, key string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, x := range m.Effects {
		if x.IdempotencyKey == key {
			n++
		}
	}
	return n, nil
}

// PostgresEffects records effects in the fake_effects table
// (docs/conductor/correctness.md, I2). The table is a test fixture, not part
// of the runtime schema, so it is created here rather than by migrations.
type PostgresEffects struct {
	db *sql.DB
}

// OpenPostgresEffects connects to dsn and creates fake_effects if needed.
func OpenPostgresEffects(ctx context.Context, dsn string) (*PostgresEffects, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("fake_effects: open: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS fake_effects (
			seq             BIGSERIAL PRIMARY KEY,
			call_id         TEXT        NOT NULL,
			idempotency_key TEXT        NOT NULL,
			tool_name       TEXT        NOT NULL,
			payload         JSONB       NOT NULL,
			observed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("fake_effects: create table: %w", err)
	}
	return &PostgresEffects{db: db}, nil
}

// Record implements EffectSink.
func (p *PostgresEffects) Record(ctx context.Context, e Effect) (int, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("fake_effects: begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		"INSERT INTO fake_effects (call_id, idempotency_key, tool_name, payload) VALUES ($1, $2, $3, $4)",
		e.CallID, e.IdempotencyKey, e.ToolName, string(e.Payload)); err != nil {
		return 0, fmt.Errorf("fake_effects: insert: %w", err)
	}
	var n int
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM fake_effects WHERE idempotency_key = $1", e.IdempotencyKey).Scan(&n); err != nil {
		return 0, fmt.Errorf("fake_effects: count: %w", err)
	}
	return n, tx.Commit()
}

// Count implements EffectSink.
func (p *PostgresEffects) Count(ctx context.Context, key string) (int, error) {
	var n int
	if err := p.db.QueryRowContext(ctx,
		"SELECT count(*) FROM fake_effects WHERE idempotency_key = $1", key).Scan(&n); err != nil {
		return 0, fmt.Errorf("fake_effects: count: %w", err)
	}
	return n, nil
}

// Close closes the database connection.
func (p *PostgresEffects) Close() error { return p.db.Close() }
