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
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	schema "github.com/simondxu/conductor/db"
)

// Postgres is the PostgreSQL event log. It keeps the runs projection and is
// the reconciler's RunIndex.
type Postgres struct {
	*sqlEventLog
}

var _ RunIndex = (*Postgres)(nil)

// OpenPostgresEventLog connects to the PostgreSQL database described by dsn and
// checks that its schema is at db.SchemaVersion. Caller is responsible to ensure
// it is safe for concurrent use.
func OpenPostgresEventLog(dsn string) (EventLog, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres_eventlog: open: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres_eventlog: ping: %w", err)
	}

	// The schema belongs to `conductor migrate`; refuse to run against any
	// other version rather than guess.
	var version int64
	var dirty bool
	if err := db.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		db.Close()
		return nil, fmt.Errorf("postgres_eventlog: reading schema version (run `conductor migrate up`): %w", err)
	}
	if dirty || version != schema.SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("postgres_eventlog: schema version %d (dirty: %t), this binary needs %d: run `conductor migrate up`",
			version, dirty, schema.SchemaVersion)
	}

	return &Postgres{&sqlEventLog{db: db, postgres: true}}, nil
}
