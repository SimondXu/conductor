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

// Package toolledger is the durable record of tool calls
// (docs/conductor/ARCHITECTURE.md §2.4, §3): one row per call with its
// arguments, class, attempt count and state, written around every execution
// so that after a crash the database alone says which calls may have had an
// effect.
package toolledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
)

// Class is a tool's declared recovery class. It is never inferred.
type Class string

const (
	Idempotent   Class = "IDEMPOTENT"
	Reconcilable Class = "RECONCILABLE"
	Unsafe       Class = "UNSAFE"
)

// ParseClass validates a declared class name.
func ParseClass(s string) (Class, error) {
	switch c := Class(s); c {
	case Idempotent, Reconcilable, Unsafe:
		return c, nil
	}
	return "", fmt.Errorf("unknown tool class %q (want IDEMPOTENT, RECONCILABLE or UNSAFE)", s)
}

// State is a call's position in the state machine of ARCHITECTURE.md §3.
type State string

const (
	Planned        State = "PLANNED"
	Started        State = "STARTED"
	Succeeded      State = "SUCCEEDED"
	Failed         State = "FAILED"
	OutcomeUnknown State = "OUTCOME_UNKNOWN"
	Abandoned      State = "ABANDONED"
)

// ErrTransition is returned for a write the state machine does not allow.
var ErrTransition = errors.New("toolledger: illegal state transition")

// ReconcileWindow is how long a started call may stay unsettled before the
// reconciler abandons it; it bounds invariant I1.
const ReconcileWindow = 15 * time.Minute

// PlannedCall is a call a model turn requested.
type PlannedCall struct {
	CallID         string
	ConversationID string
	// Step is the call's position in the conversation's step sequence.
	Step     int64
	ToolName string
	ArgsHash string
	Args     []byte // JSON object
	Class    Class
}

// Outcome is an observed result: Succeeded or Failed with the result JSON, or
// Abandoned with an error.
type Outcome struct {
	State  State
	Result []byte // JSON, nil when there is none
	Error  string
}

// ToolCall is one ledger row.
type ToolCall struct {
	PlannedCall
	IdempotencyKey string
	State          State
	Attempts       int
	Result         []byte
	Error          string
	Generation     int64
}

// Ledger records tool-call transitions. Every write carries the writer's
// ownership generation.
type Ledger interface {
	// Plan records calls as PLANNED. Re-planning an existing call_id is a
	// no-op, so replaying a turn is safe.
	Plan(ctx context.Context, gen int64, calls ...PlannedCall) error
	// Start moves a PLANNED or OUTCOME_UNKNOWN call to STARTED and increments
	// attempts. It must commit before the effect can happen.
	Start(ctx context.Context, gen int64, callID string) error
	// Settle moves a STARTED or OUTCOME_UNKNOWN call to a settled state.
	// Settling again to the same state is a no-op (duplicate callbacks).
	Settle(ctx context.Context, gen int64, callID string, out Outcome) error
	// MarkUnknown moves a STARTED call to OUTCOME_UNKNOWN: it may or may not
	// have had its effect.
	MarkUnknown(ctx context.Context, gen int64, callID, reason string) error
	// Calls returns every call of a conversation, ordered by step.
	Calls(ctx context.Context, conversationID string) ([]ToolCall, error)
	// ExpireDeadlines abandons every OUTCOME_UNKNOWN call past its
	// reconcile_deadline, which bounds I1, and returns how many it moved.
	ExpireDeadlines(ctx context.Context) (int, error)
}

// ArgsHash is the hex SHA-256 of args in canonical JSON (sorted keys).
func ArgsHash(args *structpb.Struct) (hash string, canonical []byte, err error) {
	canonical, err = json.Marshal(args.AsMap())
	if err != nil {
		return "", nil, fmt.Errorf("toolledger: canonical args: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), canonical, nil
}

// CallID is Conductor's identity for a call, recomputable from durable inputs
// alone: hex(sha256(conversation_id, step, tool_name, args_hash))[:32].
// Provider call ids are never used, because they are not stable.
func CallID(conversationID string, step int64, toolName, argsHash string) string {
	h := sha256.New()
	for _, part := range []string{conversationID, strconv.FormatInt(step, 10), toolName, argsHash} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
