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

// Package db holds Conductor's PostgreSQL schema migrations and runs them with
// golang-migrate. PostgreSQL is the only production backend; SQLite creates
// its table on open and is not migrated.
package db

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrations embed.FS

// SchemaVersion is the migration version this binary requires.
const SchemaVersion = 3

// Up applies every pending migration and returns the resulting version.
func Up(dsn string) (uint, error) {
	return run(dsn, func(m *migrate.Migrate) error { return m.Up() })
}

// Down rolls back steps migrations and returns the resulting version.
func Down(dsn string, steps int) (uint, error) {
	if steps <= 0 {
		return 0, fmt.Errorf("down needs a positive step count, got %d", steps)
	}
	return run(dsn, func(m *migrate.Migrate) error { return m.Steps(-steps) })
}

// Version returns the applied version and whether the last migration failed
// part-way (dirty). Version 0 means no migration has run.
func Version(dsn string) (version uint, dirty bool, err error) {
	m, err := open(dsn)
	if err != nil {
		return 0, false, err
	}
	defer m.Close()
	version, dirty, err = m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	return version, dirty, err
}

func run(dsn string, op func(*migrate.Migrate) error) (uint, error) {
	m, err := open(dsn)
	if err != nil {
		return 0, err
	}
	defer m.Close()
	if err := op(m); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return 0, err
	}
	v, _, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, nil
	}
	return v, err
}

func open(dsn string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations, "migrations")
	if err != nil {
		return nil, fmt.Errorf("migrations: source: %w", err)
	}
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("migrations: open: %w", err)
	}
	drv, err := pgxmigrate.WithInstance(conn, &pgxmigrate.Config{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("migrations: driver: %w", err)
	}
	return migrate.NewWithInstance("iofs", src, "pgx5", drv)
}
