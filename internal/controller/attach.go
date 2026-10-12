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
	"fmt"
	"time"

	"github.com/simondxu/conductor/proto"
)

// ErrNoConversation is returned by Attach for a conversation with no log.
var ErrNoConversation = errors.New("no such conversation")

// attachPoll is how often Attach reads the log for new steps.
const attachPoll = 100 * time.Millisecond

// pendingRetry spaces Attach's onPending calls.
const pendingRetry = time.Second

// Attach sends the conversation's logged steps after afterStep, one response
// per event with each step's Index set to the event's log step, then each
// event logged after that, and returns once the run is in a terminal state (PRD.md §6.1,
// client-stream catch-up replay). It reads only the log, the source of
// truth, so every step it sends is durable (I9) and it works on any
// replica, wherever the run executes. onPending, if set, is called when the
// run is seen PENDING, at most once per pendingRetry, so the caller can make
// sure someone drives it.
func (d *Controller) Attach(ctx context.Context, conversationID string, afterStep int64, send ExecHandler, onPending func()) error {
	// Start one event early: the event at afterStep, which the client
	// already has, carries the run's state.
	from := max(afterStep-1, 0)
	var state proto.State
	var lastPending time.Time
	for first := true; ; first = false {
		events, err := d.eventLog.EventsSince(ctx, conversationID, from)
		if err != nil {
			return err
		}
		if first && len(events) == 0 {
			if from == 0 {
				return fmt.Errorf("%w: %q", ErrNoConversation, conversationID)
			}
			return fmt.Errorf("%w: %q has no step %d", ErrNoConversation, conversationID, afterStep)
		}
		for i, ev := range events {
			step := from + 1 + int64(i) // steps are contiguous (I5)
			if ev.State != proto.State_STATE_UNSPECIFIED {
				state = ev.State
			}
			if step <= afterStep {
				continue
			}
			if len(ev.Steps) == 0 {
				continue
			}
			// One response per event: the client's cursor is the event's
			// step, so it must never hold part of an event (I11).
			for _, s := range ev.Steps {
				s.Index = step
			}
			if err := send(&proto.CreateInteractionResponse{Outputs: ev.Steps}); err != nil {
				return err
			}
		}
		from += int64(len(events))
		afterStep = max(afterStep, from)
		switch state {
		case proto.State_STATE_COMPLETED, proto.State_STATE_FAILED, proto.State_STATE_CANCELED:
			return nil
		case proto.State_STATE_PENDING:
			if onPending != nil && time.Since(lastPending) >= pendingRetry {
				lastPending = time.Now()
				onPending()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(attachPoll):
		}
	}
}
