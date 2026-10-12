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

// Package controller implements the single-writer orchestrator that coordinates
// agentic loops, manages executions, and communicates with local and remote agents.
package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/proto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

type ExecHandler func(resp *proto.CreateInteractionResponse) error

// Controller is the main controller that coordinates all components.
// It acts as a single-writer system for managing agentic loops.
type Controller struct {
	registry *Registry
	eventLog eventlog.EventLog
	ledger   toolledger.Ledger
}

// Config configures the controller.
type Config struct {
	Registry        *Registry
	EventLogBuilder eventlog.EventLogBuilder
	// Ledger records tool calls of recovery:full harnesses. Nil disables it
	// (SQLite, local use only).
	Ledger toolledger.Ledger
}

// New creates a new controller instance.
func New(ctx context.Context, cfg Config) (*Controller, error) {
	if cfg.Registry == nil {
		return nil, fmt.Errorf("registry is required")
	}
	if cfg.EventLogBuilder == nil {
		return nil, fmt.Errorf("event log builder is required")
	}
	eventLog, err := cfg.EventLogBuilder()
	if err != nil {
		return nil, fmt.Errorf("failed to create event log: %w", err)
	}

	return &Controller{
		registry: cfg.Registry,
		eventLog: eventLog,
		ledger:   cfg.Ledger,
	}, nil
}

// Exec executes a new agentic loop execution or resumes an existing one.
// If id is empty, a UUID will be generated.
// If the execution already exists, it will be resumed with optional new inputs.
func (d *Controller) Exec(ctx context.Context, req *proto.CreateInteractionEvent, handler ExecHandler) (err error) {
	runsActive.Inc()
	start := time.Now()
	defer func() {
		runsActive.Dec()
		outcome := runOutcome(err)
		runsTotal.WithLabelValues(outcome).Inc()
		runDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	}()
	return d.exec(ctx, req, handler)
}

func (d *Controller) exec(ctx context.Context, req *proto.CreateInteractionEvent, handler ExecHandler) error {
	if req.ConversationId == "" {
		return fmt.Errorf("conversation_id is required")
	}

	// TODO(jbd): Resume an incomplete execution if there exists one.
	// TODO(jbd): Enable bringing a remote harness that implements HarnessService.
	// TODO(anj): We need to consolidate agents and harness registration.
	// Adding harness registration support temporarily.
	l := newLogger(d.eventLog, req.ConversationId, req.AgentId)
	state, storedHarnessID, history, err := l.ResumptionState(ctx)
	if err != nil {
		return fmt.Errorf("failed to check resumption state: %w", err)
	}

	// On resume, use the conversation's recorded harness. Using a different harness
	// for the same conversation is not allowed.
	if req.AgentId != "" && storedHarnessID != "" && req.AgentId != storedHarnessID {
		return fmt.Errorf("resumption not allowed: harness ID changed from %s to %s", storedHarnessID, req.AgentId)
	}
	harnessID := req.AgentId
	// Use the conversations's stored harness if no harness is specified.
	if harnessID == "" {
		harnessID = storedHarnessID
	}
	// For new conversations, use the default harness if no harness is specified.
	if harnessID == "" {
		harnessID = d.registry.defaultHarness
	}
	l.harnessID = harnessID

	h, err := d.registry.Harness(harnessID)
	if err != nil {
		return fmt.Errorf("failed to get harness %q: %w", harnessID, err)
	}

	if d.registry.Recovery(harnessID) == RecoveryFull {
		return d.execOwnedLoop(ctx, req, l, harnessID, h, state, history, handler)
	}

	hhandler := &harnessHandler{
		logger:      l,
		execHandler: handler,
	}

	if state == proto.State_STATE_PENDING {
		// If the state is pending, first try to resume the
		// pending execution. If the state is COMPLETED or FAILED, start
		// a new execution.
		exec, err := h.Start(ctx, req.ConversationId, req.AgentConfig)
		if err != nil {
			return fmt.Errorf("failed to start harness session: %w", err)
		}
		defer exec.Close(ctx)

		if err := exec.Run(ctx, hhandler); err != nil {
			return fmt.Errorf("harness execution failed: %w", err)
		}
	}

	if len(req.Inputs) == 0 {
		// No more inputs, just return.
		return nil
	}

	exec, err := h.Start(ctx, req.ConversationId, req.AgentConfig)
	if err != nil {
		return fmt.Errorf("failed to start harness session: %w", err)
	}
	defer exec.Close(ctx)

	if err := exec.Queue(ctx, req.Inputs...); err != nil {
		return fmt.Errorf("failed to queue inputs: %w", err)
	}
	// Log inputs before running harness
	if _, err := l.LogInputs(ctx, req.Inputs, req.AgentConfig); err != nil {
		return fmt.Errorf("failed to log inputs: %w", err)
	}
	if err := exec.Run(ctx, hhandler); err != nil {
		return fmt.Errorf("harness execution failed: %w", err)
	}
	return nil
}

type harnessHandler struct {
	logger      *logger
	execHandler ExecHandler
}

func (a *harnessHandler) OnMessage(ctx context.Context, execID string, step *proto.Step) error {
	// Log every response received from the harness
	// TODO(anj): The harness should send the full input sent to get this particular response.
	logStep, err := a.logger.LogOutputs(ctx, []*proto.Step{step}, proto.State_STATE_PENDING)
	if err != nil {
		slog.WarnContext(ctx, "Failed to log streamed message to event log",
			slog.String("conversation_id", a.logger.conversationID),
			slog.Any("error", err),
		)
	}

	if step != nil {
		step.Index = logStep
	}

	if a.execHandler == nil {
		return nil
	}
	return a.execHandler(&proto.CreateInteractionResponse{
		Outputs: []*proto.Step{step},
	})
}

func (a *harnessHandler) OnComplete(ctx context.Context, execID string) error {
	// Mark the execution turn as completed in the conversation log
	if _, err := a.logger.LogOutputs(ctx, nil, proto.State_STATE_COMPLETED); err != nil {
		slog.WarnContext(ctx, "Failed to mark completion state in event log",
			slog.String("conversation_id", a.logger.conversationID),
			slog.Any("error", err),
		)
	}
	if a.execHandler == nil {
		return nil
	}
	return nil
}

// maxOwnedLoopTurns bounds the turns of one Exec on a recovery:full harness,
// so a model that never stops calling tools cannot loop forever.
const maxOwnedLoopTurns = 100

// ownerGeneration is the generation every ledger write carries until
// ownership leases land (M7).
const ownerGeneration = 0

// execOwnedLoop drives a recovery:full harness with the owned-loop turn
// protocol (docs/conductor/ARCHITECTURE.md §1.4): every Run receives the whole
// history and performs one turn, and the controller loops until the history no
// longer needs a turn.
//
// Tool calls get Conductor's own call_id as they are emitted. With a ledger,
// each call is recorded around its execution: PLANNED after the model turn
// that requested it, STARTED before the tool turn that runs it, then settled
// from its result, or OUTCOME_UNKNOWN when that turn ends without one.
func (d *Controller) execOwnedLoop(ctx context.Context, req *proto.CreateInteractionEvent, l *logger, harnessID string, h harness.Harness, state proto.State, history []*proto.Step, handler ExecHandler) error {
	if len(req.Inputs) == 0 && state != proto.State_STATE_PENDING {
		return nil
	}
	// Re-running a tool without the reconciler could repeat an UNSAFE effect,
	// so an unanswered call is left for the reconciler (M4) instead.
	if calls := harness.UnansweredToolCalls(history); len(calls) > 0 {
		return fmt.Errorf("conversation %q has %d unanswered tool call(s); resuming it needs the reconciler", req.ConversationId, len(calls))
	}

	if len(req.Inputs) > 0 {
		if _, err := l.LogInputs(ctx, req.Inputs, req.AgentConfig); err != nil {
			return fmt.Errorf("failed to log inputs: %w", err)
		}
		history = append(history, req.Inputs...)
	}

	exec, err := h.Start(ctx, req.ConversationId, req.AgentConfig)
	if err != nil {
		return fmt.Errorf("failed to start harness session: %w", err)
	}
	defer exec.Close(ctx)

	th := &turnHandler{harnessHandler: harnessHandler{logger: l, execHandler: handler}, conversationID: req.ConversationId}
	for turn := 0; harness.NeedsTurn(history); turn++ {
		if turn == maxOwnedLoopTurns {
			return fmt.Errorf("harness did not finish within %d turns", maxOwnedLoopTurns)
		}
		th.base, th.emitted, th.planned = len(history), nil, nil
		pending := harness.UnansweredToolCalls(history)
		if err := d.startCalls(ctx, pending); err != nil {
			return err
		}
		if err := exec.Queue(ctx, history...); err != nil {
			return fmt.Errorf("failed to queue history: %w", err)
		}
		runErr := exec.Run(ctx, th)
		// Ledger writes after a failed turn must survive the cancellation that
		// may have failed it.
		lctx := context.WithoutCancel(ctx)
		if err := d.planCalls(lctx, harnessID, th.planned); err != nil {
			return err
		}
		if err := d.settleCalls(lctx, pending, th.emitted, runErr); err != nil {
			return err
		}
		if runErr != nil {
			return fmt.Errorf("harness execution failed: %w", runErr)
		}
		if len(th.emitted) == 0 {
			return fmt.Errorf("harness turn %d produced no steps", turn)
		}
		history = append(history, th.emitted...)
	}
	if _, err := l.LogOutputs(ctx, nil, proto.State_STATE_COMPLETED); err != nil {
		return fmt.Errorf("failed to log completion: %w", err)
	}
	return nil
}

func (d *Controller) planCalls(ctx context.Context, harnessID string, calls []toolledger.PlannedCall) error {
	if d.ledger == nil || len(calls) == 0 {
		return nil
	}
	for i := range calls {
		calls[i].Class = d.registry.ToolClass(harnessID, calls[i].ToolName)
	}
	if err := d.ledger.Plan(ctx, ownerGeneration, calls...); err != nil {
		return fmt.Errorf("failed to plan tool calls: %w", err)
	}
	return nil
}

// startCalls marks the calls of the coming tool turn STARTED. If one cannot
// start, the turn does not run, and the calls already started are marked
// OUTCOME_UNKNOWN: there is no way back to PLANNED, and the ledger must not
// claim more than it knows.
func (d *Controller) startCalls(ctx context.Context, calls []*proto.ToolCallStep) error {
	if d.ledger == nil {
		return nil
	}
	for i, c := range calls {
		if err := d.ledger.Start(ctx, ownerGeneration, c.GetId()); err != nil {
			for _, started := range calls[:i] {
				_ = d.ledger.MarkUnknown(context.WithoutCancel(ctx), ownerGeneration, started.GetId(), "tool turn not run: "+err.Error())
			}
			return fmt.Errorf("failed to start tool call %s: %w", c.GetId(), err)
		}
	}
	return nil
}

// settleCalls settles the calls of a tool turn from the results it emitted.
// A call without a result may or may not have had its effect: OUTCOME_UNKNOWN.
func (d *Controller) settleCalls(ctx context.Context, calls []*proto.ToolCallStep, emitted []*proto.Step, runErr error) error {
	if d.ledger == nil || len(calls) == 0 {
		return nil
	}
	results := map[string]*proto.ToolResultStep{}
	for _, s := range emitted {
		if r := s.GetToolResult(); r != nil {
			results[r.GetCallId()] = r // the last result for a call wins
		}
	}
	for _, c := range calls {
		r, ok := results[c.GetId()]
		if !ok {
			reason := "tool turn ended without a result"
			if runErr != nil {
				reason = "tool turn failed: " + runErr.Error()
			}
			if err := d.ledger.MarkUnknown(ctx, ownerGeneration, c.GetId(), reason); err != nil {
				return fmt.Errorf("failed to mark tool call %s unknown: %w", c.GetId(), err)
			}
			continue
		}
		result, err := protojson.Marshal(r)
		if err != nil {
			return fmt.Errorf("failed to encode result of %s: %w", c.GetId(), err)
		}
		out := toolledger.Outcome{State: toolledger.Succeeded, Result: result}
		if r.GetFunctionResult().GetIsError() {
			out.State = toolledger.Failed
		}
		if err := d.ledger.Settle(ctx, ownerGeneration, c.GetId(), out); err != nil {
			return fmt.Errorf("failed to settle tool call %s: %w", c.GetId(), err)
		}
	}
	return nil
}

// turnHandler logs and forwards each step of one owned-loop turn and records
// it for the next turn's history. It replaces each tool call's id with
// Conductor's call_id before the call is logged or delivered. Completion is
// written once by execOwnedLoop, not per turn.
type turnHandler struct {
	harnessHandler
	conversationID string
	base           int // len(history) when the turn started
	emitted        []*proto.Step
	planned        []toolledger.PlannedCall
}

func (t *turnHandler) OnMessage(ctx context.Context, execID string, step *proto.Step) error {
	if call := step.GetToolCall(); call != nil {
		fc := call.GetFunctionCall()
		hash, canonical, err := toolledger.ArgsHash(fc.GetArguments())
		if err != nil {
			return err
		}
		pos := int64(t.base + len(t.emitted))
		call.Id = toolledger.CallID(t.conversationID, pos, fc.GetName(), hash)
		t.planned = append(t.planned, toolledger.PlannedCall{
			CallID: call.Id, ConversationID: t.conversationID, Step: pos,
			ToolName: fc.GetName(), ArgsHash: hash, Args: canonical,
		})
	}
	t.emitted = append(t.emitted, step)
	return t.harnessHandler.OnMessage(ctx, execID, step)
}

func (t *turnHandler) OnComplete(ctx context.Context, execID string) error { return nil }

// Registry returns the agent registry.
func (d *Controller) Registry() *Registry {
	return d.registry
}

// Close gracefully shuts down the controller.
func (d *Controller) Close() error {
	if err := d.eventLog.Close(); err != nil {
		return fmt.Errorf("failed to close event log: %w", err)
	}
	if err := d.registry.Close(); err != nil {
		return fmt.Errorf("failed to close registry: %w", err)
	}
	if c, ok := d.ledger.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return fmt.Errorf("failed to close tool ledger: %w", err)
		}
	}
	return nil
}

type logger struct {
	conversationID string
	interactionID  string
	el             eventlog.EventLog
	harnessID      string
}

func newLogger(
	el eventlog.EventLog,
	conversationID string,
	harnessID string) *logger {
	return &logger{
		el:             el,
		conversationID: conversationID,
		harnessID:      harnessID,
	}
}

// ResumptionState returns the conversation's current state, the harness it
// used, and every step logged so far, in order.
func (l *logger) ResumptionState(ctx context.Context) (proto.State, string, []*proto.Step, error) {
	events, err := l.el.Events(ctx, l.conversationID)
	if err != nil {
		return proto.State_STATE_UNSPECIFIED, "", nil, err
	}

	var state proto.State
	var harnessID string
	var steps []*proto.Step
	for _, ev := range events {
		if harnessID == "" && ev.AgentId != "" {
			harnessID = ev.AgentId
		}
		if ev.State != proto.State_STATE_UNSPECIFIED {
			state = ev.State
		}
		steps = append(steps, ev.Steps...)
	}
	return state, harnessID, steps, nil
}

func (l *logger) LogInputs(ctx context.Context, steps []*proto.Step, agentConfig []byte) (int64, error) {
	// Parse the agent config into a human-readable struct for logging.
	var cfg *structpb.Struct
	if len(agentConfig) > 0 {
		cfg = &structpb.Struct{}
		if err := protojson.Unmarshal(agentConfig, cfg); err != nil {
			slog.WarnContext(ctx, "Failed to parse agent config for logging",
				slog.String("conversation_id", l.conversationID),
				slog.Any("error", err),
			)
			cfg = nil
		}
	}
	ev := &proto.StepEvent{
		ConversationId: l.conversationID,
		InteractionId:  l.interactionID,
		AgentId:        l.harnessID,
		AgentConfig:    cfg,
		Steps:          steps,
		State:          proto.State_STATE_PENDING,
	}
	return l.el.Append(ctx, ev)
}

func (l *logger) LogOutputs(ctx context.Context, steps []*proto.Step, state proto.State) (int64, error) {
	ev := &proto.StepEvent{
		ConversationId: l.conversationID,
		InteractionId:  l.interactionID,
		Steps:          steps,
		State:          state,
	}
	return l.el.Append(ctx, ev)
}
