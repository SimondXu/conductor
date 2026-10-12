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
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// handoverChannel is the PostgreSQL NOTIFY channel of handovers.
const handoverChannel = "conductor_handover"

// Handover tells every replica listening on Handovers that conversationID
// was released unfinished, so one can take it over now instead of at its
// next reconciler pass (ARCHITECTURE.md §8.1: graceful handover should not
// wait). Send it after the Release.
func (p *Postgres) Handover(ctx context.Context, conversationID string) error {
	if _, err := p.db.ExecContext(ctx, "SELECT pg_notify($1, $2)", handoverChannel, conversationID); err != nil {
		return fmt.Errorf("lease: handover %s: %w", conversationID, err)
	}
	return nil
}

// Handovers delivers a signal for handovers sent by any replica, coalescing
// those that arrive while one is pending, until ctx ends. It holds one
// connection of the pool to LISTEN on, and reconnects after a failure.
func (p *Postgres) Handovers(ctx context.Context) <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		for ctx.Err() == nil {
			if err := p.listen(ctx, ch); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "Listening for lease handovers failed; retrying", slog.Any("error", err))
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
		}
	}()
	return ch
}

func (p *Postgres) listen(ctx context.Context, ch chan<- struct{}) error {
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(driverConn any) error {
		c := driverConn.(*stdlib.Conn).Conn()
		// Never return a LISTENing connection to the pool: the pool drops
		// a closed one.
		defer c.Close(context.WithoutCancel(ctx))
		if _, err := c.Exec(ctx, "LISTEN "+handoverChannel); err != nil {
			return err
		}
		for {
			if _, err := c.WaitForNotification(ctx); err != nil {
				return err
			}
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	})
}
