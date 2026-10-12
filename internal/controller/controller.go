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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/simondxu/conductor/internal/controller/eventlog"
	"github.com/simondxu/conductor/internal/controller/lease"
	"github.com/simondxu/conductor/internal/controller/toolledger"
	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/telemetry"
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
	lease    lease.Lease
	ownerID  string
	leaseTTL time.Duration
}

// Config configures the controller.
type Config struct {
	Registry        *Registry
	EventLogBuilder eventlog.EventLogBuilder
	// Ledger records tool calls of recovery:full harnesses. Nil disables it
	// (SQLite, local use only).
	Ledger toolledger.Ledger
	// Lease, when set, makes every Exec own its conversation for its
	// duration (ARCHITECTURE.md §5.1), so a client and the reconciler never
	// drive the same run at once. Nil is process-local ownership (SQLite).
	Lease lease.Lease
	// OwnerID names this controller in the lease table.
	OwnerID string
	// LeaseTTL defaults to DefaultLeaseTTL.
	LeaseTTL time.Duration
}

// DefaultLeaseTTL is the lease ttl; holders renew at a third of it
// (ARCHITECTURE.md §10).
const DefaultLeaseTTL = 30 * time.Second

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

	if cfg.Lease != nil && cfg.OwnerID == "" {
		return nil, fmt.Errorf("owner id is required with a lease")
	}
	ownerID := cfg.OwnerID
	if ownerID == "" {
		ownerID = "local" // process-local ownership (SQLite)
	}
	ttl := cfg.LeaseTTL
	if ttl == 0 {
		ttl = DefaultLeaseTTL
	}
	return &Controller{
		registry: cfg.Registry,
		eventLog: eventLog,
		ledger:   cfg.Ledger,
		lease:    cfg.Lease,
		ownerID:  ownerID,
		leaseTTL: ttl,
	}, nil
}

// Exec executes a new agentic loop execution or resumes an existing one.
// If id is empty, a UUID will be generated.
// If the execution already exists, it will be resumed with optional new inputs.
func (d *Controller) Exec(ctx context.Context, req *proto.CreateInteractionEvent, handler ExecHandler) (err error) {
	runsActive.WithLabelValues(d.ownerID).Inc()
	start := time.Now()
	defer func() {
		runsActive.WithLabelValues(d.ownerID).Dec()
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
	ctx = telemetry.WithLogAttrs(ctx, slog.String("conversation_id", req.ConversationId), slog.String("owner_id", d.ownerID))
	ctx, gen, release, err := d.own(ctx, req.ConversationId)
	if err != nil {
		return err
	}
	defer release()
	ctx = telemetry.WithLogAttrs(ctx, slog.Int64("generation", gen))

	// TODO(jbd): Resume an incomplete execution if there exists one.
	// TODO(jbd): Enable bringing a remote harness that implements HarnessService.
	// TODO(anj): We need to consolidate agents and harness registration.
	// Adding harness registration support temporarily.
	l := newLogger(d.eventLog, req.ConversationId, req.AgentId)
	events, err := d.eventLog.Events(ctx, req.ConversationId)
	if err != nil {
		return fmt.Errorf("failed to check resumption state: %w", err)
	}
	replayEvents.Add(float64(len(events)))
	st, err := rebuild(events)
	if err != nil {
		return fmt.Errorf("failed to rebuild conversation %q: %w", req.ConversationId, err)
	}
	if len(req.Inputs) == 0 {
		slog.InfoContext(ctx, "recovery.rebuild", slog.Int("events", len(events)),
			slog.Int("steps", len(st.History)), slog.String("state", st.State.String()))
	}
	state, storedHarnessID := st.State, st.HarnessID

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
		return d.execOwnedLoop(ctx, req, l, gen, harnessID, h, st, handler)
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
			l.logFailure(ctx, err)
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
		l.logFailure(ctx, err)
		return fmt.Errorf("harness execution failed: %w", err)
	}
	return nil
}

// own takes the conversation's lease for the duration of one Exec and returns
// a context that ends when the lease is lost, the generation every ledger
// write carries, and the release to defer. Without a lease it is a no-op at
// generation 0. lease.ErrHeld means another controller or client is driving
// the conversation.
func (d *Controller) own(ctx context.Context, conversationID string) (context.Context, int64, func(), error) {
	if d.lease == nil {
		return ctx, 0, func() {}, nil
	}
	held, err := d.lease.Acquire(ctx, conversationID, d.ownerID, d.leaseTTL)
	if err != nil {
		return nil, 0, nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-held.Done(): // lost: stop executing (ARCHITECTURE.md §6.2)
			cancel()
		case <-ctx.Done():
		}
	}()
	go lease.KeepAlive(ctx, held, d.leaseTTL/3)
	return ctx, held.Generation(), func() {
		cancel()
		if err := held.Release(context.WithoutCancel(ctx)); err != nil && !errors.Is(err, lease.ErrLost) {
			slog.WarnContext(ctx, "Failed to release the conversation lease", slog.Any("error", err))
		}
	}, nil
}

type harnessHandler struct {
	logger      *logger
	execHandler ExecHandler
}

func (a *harnessHandler) OnMessage(ctx context.Context, execID string, step *proto.Step) error {
	if err := a.log(ctx, step); err != nil {
		return err
	}
	return a.deliver(step)
}

// log appends step to the conversation log.
func (a *harnessHandler) log(ctx context.Context, step *proto.Step) error {
	// Log every response received from the harness
	// TODO(anj): The harness should send the full input sent to get this particular response.
	logStep, err := a.logger.LogOutputs(ctx, []*proto.Step{step}, proto.State_STATE_PENDING)
	if err != nil {
		// I9: a step that is not durable is never delivered.
		return fmt.Errorf("failed to log streamed step: %w", err)
	}

	if step != nil {
		step.Index = logStep
	}
	return nil
}

// deliver sends a logged step to the client.
func (a *harnessHandler) deliver(step *proto.Step) error {
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
		return fmt.Errorf("failed to log completion: %w", err)
	}
	return nil
}

// maxOwnedLoopTurns bounds the turns of one Exec on a recovery:full harness,
// so a model that never stops calling tools cannot loop forever.
const maxOwnedLoopTurns = 100

// execOwnedLoop drives a recovery:full harness with the owned-loop turn
// protocol (docs/conductor/ARCHITECTURE.md §1.4): every Run receives the whole
// history and performs one turn, and the controller loops until the history no
// longer needs a turn.
//
// Tool calls get Conductor's own call_id as they are emitted. With a ledger,
// each call is recorded around its execution: PLANNED after the model turn
// that requested it, STARTED before the tool turn that runs it, then settled
// from its result, or OUTCOME_UNKNOWN when that turn ends without one.
//
// A resumed run continues from the history in the log, with the logged
// agent_config unless the request brings one. Calls it left unanswered are
// first resolved from the ledger by recoverCalls.
func (d *Controller) execOwnedLoop(ctx context.Context, req *proto.CreateInteractionEvent, l *logger, gen int64, harnessID string, h harness.Harness, st runState, handler ExecHandler) error {
	if len(req.Inputs) == 0 && st.State != proto.State_STATE_PENDING {
		return nil
	}
	history := st.History
	if calls := harness.UnansweredToolCalls(history); len(calls) > 0 {
		// Without the ledger nothing says whether a call had its effect, and
		// re-running it could repeat an UNSAFE one.
		if d.ledger == nil {
			return fmt.Errorf("conversation %q has %d unanswered tool call(s); resuming it needs the tool ledger (PostgreSQL)", req.ConversationId, len(calls))
		}
		if len(req.Inputs) > 0 {
			return fmt.Errorf("conversation %q has an unfinished run; resume it before sending new input", req.ConversationId)
		}
	}
	if d.ledger != nil && st.State == proto.State_STATE_PENDING {
		failed, err := d.recoverCalls(ctx, gen, req.ConversationId, harnessID, history)
		if err != nil {
			return err
		}
		if len(failed) > 0 {
			// The error results and FAILED go in one event, so the run cannot
			// be left answered but still PENDING.
			if _, err := l.LogOutputs(ctx, failed, proto.State_STATE_FAILED); err != nil {
				return fmt.Errorf("failed to log the failed run: %w", err)
			}
			return errors.New("run failed: a tool call's outcome is unknown and it cannot be safely re-run")
		}
	}
	agentConfig := req.AgentConfig
	if len(agentConfig) == 0 {
		agentConfig = st.AgentConfig
	}

	if len(req.Inputs) > 0 {
		if _, err := l.LogInputs(ctx, req.Inputs, req.AgentConfig); err != nil {
			return fmt.Errorf("failed to log inputs: %w", err)
		}
		history = append(history, req.Inputs...)
	}

	exec, err := h.Start(ctx, req.ConversationId, agentConfig)
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
		if err := d.startCalls(ctx, gen, pending); err != nil {
			return err
		}
		if err := exec.Queue(ctx, history...); err != nil {
			return fmt.Errorf("failed to queue history: %w", err)
		}
		runErr := exec.Run(ctx, th)
		// Ledger writes after a failed turn must survive the cancellation that
		// may have failed it.
		lctx := context.WithoutCancel(ctx)
		if err := d.planCalls(lctx, gen, harnessID, th.planned); err != nil {
			return err
		}
		if err := d.settleCalls(lctx, gen, pending, th.emitted, runErr); err != nil {
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

func (d *Controller) planCalls(ctx context.Context, gen int64, harnessID string, calls []toolledger.PlannedCall) error {
	if d.ledger == nil || len(calls) == 0 {
		return nil
	}
	for i := range calls {
		calls[i].Class = d.registry.ToolClass(harnessID, calls[i].ToolName)
	}
	if err := d.ledger.Plan(ctx, gen, calls...); err != nil {
		return fmt.Errorf("failed to plan tool calls: %w", err)
	}
	return nil
}

// startCalls marks the calls of the coming tool turn STARTED. If one cannot
// start, the turn does not run, and the calls already started are marked
// OUTCOME_UNKNOWN: there is no way back to PLANNED, and the ledger must not
// claim more than it knows.
func (d *Controller) startCalls(ctx context.Context, gen int64, calls []*proto.ToolCallStep) error {
	if d.ledger == nil {
		return nil
	}
	for i, c := range calls {
		if err := d.ledger.Start(ctx, gen, c.GetId()); err != nil {
			for _, started := range calls[:i] {
				_ = d.ledger.MarkUnknown(context.WithoutCancel(ctx), gen, started.GetId(), "tool turn not run: "+err.Error())
			}
			return fmt.Errorf("failed to start tool call %s: %w", c.GetId(), err)
		}
	}
	return nil
}

// settleCalls settles the calls of a tool turn from the results it emitted.
// A call without a result may or may not have had its effect: OUTCOME_UNKNOWN.
func (d *Controller) settleCalls(ctx context.Context, gen int64, calls []*proto.ToolCallStep, emitted []*proto.Step, runErr error) error {
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
			if err := d.ledger.MarkUnknown(ctx, gen, c.GetId(), reason); err != nil {
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
		if err := d.ledger.Settle(ctx, gen, c.GetId(), out); err != nil {
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
		p, err := plannedCall(t.conversationID, int64(t.base+len(t.emitted)), call)
		if err != nil {
			return err
		}
		call.Id = p.CallID
		t.planned = append(t.planned, p)
	}
	if err := t.log(ctx, step); err != nil {
		return err
	}
	// Only a logged step is part of the history; a result that never reached
	// the log must not settle its call (I7b).
	t.emitted = append(t.emitted, step)
	return t.deliver(step)
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
	if c, ok := d.lease.(io.Closer); ok {
		if err := c.Close(); err != nil {
			return fmt.Errorf("failed to close leases: %w", err)
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

func (l *logger) LogInputs(ctx context.Context, steps []*proto.Step, agentConfig []byte) (int64, error) {
	// Parse the agent config into a human-readable struct for logging.
	var cfg *structpb.Struct
	if len(agentConfig) > 0 {
		cfg = &structpb.Struct{}
		if err := protojson.Unmarshal(agentConfig, cfg); err != nil {
			slog.WarnContext(ctx, "Failed to parse agent config for logging",
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
	return l.append(ctx, ev)
}

func (l *logger) LogOutputs(ctx context.Context, steps []*proto.Step, state proto.State) (int64, error) {
	ev := &proto.StepEvent{
		ConversationId: l.conversationID,
		InteractionId:  l.interactionID,
		Steps:          steps,
		State:          state,
	}
	return l.append(ctx, ev)
}

// appendAttempts bounds how often one event's append is tried. Retrying is
// safe because every attempt carries the same event_id (I10).
const appendAttempts = 3

// append gives ev an event_id and appends it, retrying transient failures.
func (l *logger) append(ctx context.Context, ev *proto.StepEvent) (int64, error) {
	ev.EventId = uuid.NewString()
	var err error
	for attempt := range appendAttempts {
		var step int64
		if step, err = l.el.Append(ctx, ev); err == nil {
			return step, nil
		}
		if attempt == appendAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return 0, err
		case <-time.After(time.Duration(50<<attempt) * time.Millisecond):
		}
	}
	return 0, err
}

// logFailure records the end of a failed turn, so the conversation is not left
// PENDING forever (docs/conductor/streaming-path.md §5.1): CANCELED when the
// caller went away, FAILED otherwise. Best effort: if it cannot be written,
// the conversation stays PENDING, as before.
func (l *logger) logFailure(ctx context.Context, runErr error) {
	state := proto.State_STATE_FAILED
	if errors.Is(runErr, context.Canceled) || ctx.Err() != nil {
		state = proto.State_STATE_CANCELED
	}
	if _, err := l.LogOutputs(context.WithoutCancel(ctx), nil, state); err != nil {
		slog.WarnContext(ctx, "Failed to record the failed turn",
			slog.Any("error", err),
		)
	}
}
