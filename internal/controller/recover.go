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
	"fmt"
	"log/slog"

	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
)

// errAbandoned is the ledger error of a call abandoned on recovery.
const errAbandoned = "outcome unknown and the tool cannot be safely re-run"

// recoverCalls brings the ledger of a resumed conversation in line with its
// history (ARCHITECTURE.md §6.1, PRD.md §6.3). A call whose result is in the
// log but whose owner died before settling it is settled from that result,
// the last one winning. For the calls the history leaves unanswered, one
// still STARTED was cut off by a crash and becomes OUTCOME_UNKNOWN first.
// Then:
//
//   - PLANNED calls, and IDEMPOTENT calls whose outcome is unknown, are left
//     for the next tool turn, which runs them under their idempotency key.
//   - A RECONCILABLE call whose outcome is unknown stops the resume: nothing
//     can query its external state yet, so it waits for its deadline.
//   - An UNSAFE call whose outcome is unknown is ABANDONED, and the run fails.
//
// When the run fails, every unknown call is abandoned and failed is one error
// result per unanswered call, to be logged with the FAILED state so the
// history stays answerable.
func (d *Controller) recoverCalls(ctx context.Context, gen int64, conversationID, harnessID string, history []*proto.Step) (failed []*proto.Step, err error) {
	rows, err := d.ledger.Calls(ctx, conversationID)
	if err != nil {
		return nil, fmt.Errorf("failed to read the tool ledger: %w", err)
	}
	byID := make(map[string]toolledger.ToolCall, len(rows))
	for _, r := range rows {
		byID[r.CallID] = r
	}
	unanswered := map[string]bool{}
	for _, c := range harness.UnansweredToolCalls(history) {
		unanswered[c.GetId()] = true
	}

	var answered, calls []*proto.ToolCallStep
	var unplanned []toolledger.PlannedCall
	var unknown []toolledger.ToolCall
	fail, wait := false, ""
	for pos, s := range history {
		c := s.GetToolCall()
		if c == nil {
			continue
		}
		if !unanswered[c.GetId()] {
			if row, ok := byID[c.GetId()]; ok && (row.State == toolledger.Started || row.State == toolledger.OutcomeUnknown) {
				answered = append(answered, c)
			}
			continue
		}
		calls = append(calls, c)
		row, ok := byID[c.GetId()]
		if !ok {
			// The process died between logging the call and planning it.
			p, err := plannedCall(conversationID, int64(pos), c)
			if err != nil {
				return nil, err
			}
			p.CallID = c.GetId()
			unplanned = append(unplanned, p)
			continue
		}
		switch row.State {
		case toolledger.Planned:
			continue
		case toolledger.Abandoned:
			fail = true // abandoned by an earlier recovery that died before logging it
			continue
		case toolledger.Started:
			if err := d.ledger.MarkUnknown(ctx, gen, row.CallID, "owner crashed in flight"); err != nil {
				return nil, fmt.Errorf("failed to mark tool call %s unknown: %w", row.CallID, err)
			}
		case toolledger.OutcomeUnknown:
		default:
			return nil, fmt.Errorf("tool call %s is %s in the ledger but has no result in the log", row.CallID, row.State)
		}
		unknown = append(unknown, row)
		switch row.Class {
		case toolledger.Idempotent:
		case toolledger.Reconcilable:
			wait = row.CallID
		default:
			fail = true
		}
	}
	if err := d.settleCalls(ctx, gen, answered, history, nil); err != nil {
		return nil, err
	}
	if err := d.planCalls(ctx, gen, harnessID, unplanned); err != nil {
		return nil, err
	}
	if len(answered)+len(calls) > 0 {
		slog.InfoContext(ctx, "recovery.settle", slog.Int("settled_from_log", len(answered)),
			slog.Int("unanswered", len(calls)), slog.Int("unplanned", len(unplanned)),
			slog.Int("outcome_unknown", len(unknown)), slog.Bool("run_fails", fail), slog.String("waiting_on", wait))
	}
	if !fail {
		if wait != "" {
			return nil, fmt.Errorf("tool call %s is RECONCILABLE with an unknown outcome; its external state cannot be queried yet, so it stays OUTCOME_UNKNOWN until its reconcile_deadline", wait)
		}
		return nil, nil
	}

	for _, row := range unknown {
		if err := d.ledger.Settle(ctx, gen, row.CallID, toolledger.Outcome{State: toolledger.Abandoned, Error: errAbandoned}); err != nil {
			return nil, fmt.Errorf("failed to abandon tool call %s: %w", row.CallID, err)
		}
	}
	for _, c := range calls {
		failed = append(failed, errorResult(c, "not completed: the run failed because a tool call's outcome is unknown and it cannot be safely re-run"))
	}
	return failed, nil
}

// plannedCall is the ledger entry of a call at position pos of the
// conversation's steps, with Conductor's call_id.
func plannedCall(conversationID string, pos int64, call *proto.ToolCallStep) (toolledger.PlannedCall, error) {
	fc := call.GetFunctionCall()
	hash, canonical, err := toolledger.ArgsHash(fc.GetArguments())
	if err != nil {
		return toolledger.PlannedCall{}, err
	}
	return toolledger.PlannedCall{
		CallID:         toolledger.CallID(conversationID, pos, fc.GetName(), hash),
		ConversationID: conversationID, Step: pos,
		ToolName: fc.GetName(), ArgsHash: hash, Args: canonical,
	}, nil
}

// errorResult is an error ToolResultStep for call.
func errorResult(call *proto.ToolCallStep, msg string) *proto.Step {
	return &proto.Step{Type: &proto.Step_ToolResult{ToolResult: &proto.ToolResultStep{
		CallId: call.GetId(),
		Type: &proto.ToolResultStep_FunctionResult{FunctionResult: &proto.FunctionResultStep{
			Name:    call.GetFunctionCall().GetName(),
			IsError: true,
			Result:  &proto.Value{Kind: &proto.Value_StringValue{StringValue: msg}},
		}},
	}}}
}
