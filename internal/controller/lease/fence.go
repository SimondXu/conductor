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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ErrFenced is returned for a write whose generation is not the
// conversation's current one (correctness.md I4): another owner has taken the
// conversation over, and the writer must stop.
var ErrFenced = errors.New("lease: write fenced by a newer generation")

var fencedWrites = promauto.NewCounter(prometheus.CounterOpts{
	Name: "conductor_fenced_writes_rejected_total",
	Help: "Writes the database rejected because they carried a stale generation (I4).",
})

// CheckFence fences a write inside tx: it locks the conversation's lease row
// for share, so no takeover can commit until tx ends, and returns ErrFenced
// unless gen is the row's generation. A conversation that was never leased
// has generation 0. Run it before the write, in the write's transaction.
func CheckFence(ctx context.Context, tx *sql.Tx, conversationID string, gen int64) error {
	var current int64
	err := tx.QueryRowContext(ctx,
		"SELECT generation FROM conversation_leases WHERE conversation_id = $1 FOR SHARE", conversationID).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("lease: check fence %s: %w", conversationID, err)
	}
	if current != gen {
		fencedWrites.Inc()
		return fmt.Errorf("%w: %s written at generation %d, current %d", ErrFenced, conversationID, gen, current)
	}
	return nil
}
