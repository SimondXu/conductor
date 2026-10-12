# Conductor — Correctness

> **Authoritative invariant set.** `ARCHITECTURE.md` §4 and `PLAN.md` §4 both
> defer to this file. Block (6) asserts each invariant individually, by the
> named query or test — never by "did it recover".
>
> Every invariant below states how it is **falsified**. An invariant that cannot
> be falsified is a slogan, and does not belong here.

---

## 0. Scope: what Conductor can and cannot guarantee

Three boundaries, stated before the invariants because every one of them
depends on these.

### 0.1 No exactly-once external side effects

**Conductor does not claim exactly-once external side effects, and never
will.** No system that calls someone else's API over a network can promise it.
The guarantee is: after any crash, durable state alone determines whether a
side effect *may* have happened, and the recovery policy for that tool class is
applied deliberately rather than by accident.

### 0.2 At most one *committing* writer, not one *running* process

Fencing stops stale **writes**. It does not stop a stale **process**.

A controller that has lost its lease can still be mid-`executeTool`, and a
Kubernetes pod that has been deleted keeps running until its grace period
elapses. Ownership transfer therefore guarantees:

> at most one owner can **commit** state for a conversation,

not

> at most one process is **running** for a conversation.

The gap between the two is exactly the window that produces
`OUTCOME_UNKNOWN`. That is the honest shape of the guarantee, and it is why the
ledger exists.

### 0.3 The recoverable-harness contract

Controller-level recovery can only close the loop when **Conductor owns the
tool-execution loop** — the harness returns tool calls and accepts tool results
as *input*.

This is not a preference, it is forced by upstream's protocol.
`internal/harness/antigravityinteractions/server.go:143-151` states it as a
deliberate design decision:

> mid-run steering (injecting extra human input while a turn is running) is
> intentionally NOT supported here. The execution can accept steering via
> Queue, but the `HarnessRequest` oneof only defines {start, cancel} — there is
> no wire frame to carry additional input after start

`proto/ax.proto:50-56` confirms the oneof is `{start, cancel}`.

Consequence: for a harness that runs tools internally, a tool reconciled to
`SUCCEEDED` during recovery **cannot be delivered back to the model**. The
model's conversation state lives remotely and is still waiting on a
`function_result` for that `call_id`. Such a run would continue permanently out
of step.

Harnesses are therefore classified in the registry:

| Harness | `recovery` | Why |
|---|---|---|
| `fake` | `full` | Conductor owns the tool loop |
| `openaicompat` | `full` | Conductor owns the tool loop |
| `antigravity` | `none` | tool loop inside the Python sidecar |
| `antigravityinteractions` | `none` | tool loop in-process in the harness; model state remote |
| `substrate` | `none` | tool loop in the remote actor |

The reconciler **skips** `recovery: none` conversations and records
`conductor_recovery_skipped_total{reason="harness_not_recoverable"}`. Invariants
I1, I1b, I2, I6 and I8 are asserted only for `recovery: full` harnesses. This
scoping is stated in the README, not hidden.

---

## 1. Invariants

### I1 — Every started tool call settles, within a bound

> Every `tool_calls` row that reached `STARTED` eventually reaches a settled
> state: `SUCCEEDED`, `FAILED`, or `ABANDONED`.

`OUTCOME_UNKNOWN` is **not** settled. It is a durable resting state that the
reconciler picks up again; it is never an in-memory step between two function
calls, because the new owner can crash inside that step too.

Without a bound this invariant is vacuous: a `RECONCILABLE` call whose external
system is permanently down would sit in `OUTCOME_UNKNOWN` forever, never
violating I1 and never satisfying it. So each call carries a
`reconcile_deadline`. On expiry it moves to `ABANDONED` with
`error = 'reconcile_deadline_exceeded'`.

**Falsified by**

```sql
-- any unsettled call past its deadline
SELECT call_id, state, attempts, reconcile_deadline
  FROM tool_calls
 WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN')
   AND reconcile_deadline < now();
```

Non-empty after the bench's drain phase ⇒ violation.

### I1b — A run cannot complete over an unsettled call

> No run is `COMPLETED` while any of its `tool_calls` is `STARTED` or
> `OUTCOME_UNKNOWN`.

**Falsified by**

```sql
SELECT r.conversation_id, t.call_id, t.state
  FROM runs r
  JOIN tool_calls t USING (conversation_id)
 WHERE r.state = 'COMPLETED'
   AND t.state IN ('STARTED', 'OUTCOME_UNKNOWN');
```

### I2 — Idempotent retries do not multiply effects

> For a tool whose class is `IDEMPOTENT`, the observed external side-effect
> count per `idempotency_key` never exceeds one, regardless of crash boundary or
> attempt count.

Detected mechanically, not by inspection. The `fake` harness appends every side
effect it performs to a test-fixture table:

```sql
CREATE TABLE fake_effects (
    seq             BIGSERIAL PRIMARY KEY,
    call_id         TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    tool_name       TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    observed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Falsified by**

```sql
SELECT e.idempotency_key, count(*)
  FROM fake_effects e
  JOIN tool_calls t USING (call_id)
 WHERE t.class = 'IDEMPOTENT'
 GROUP BY e.idempotency_key
HAVING count(*) > 1;
```

`conductor_duplicate_effects_total` is the runtime counterpart of this query.

`fake_effects` belongs to the test fixture. It is never part of the runtime.

### I3 — (design property) Single committing owner

> At most one controller holds a non-expired lease for a conversation at any
> instant.

**This is a design property, not an assertion.** "At any instant" is a
statement about global simultaneity that no observer in the system can sample.
Asserting it directly would be theatre. What is observable is its consequence,
I3′, plus I4.

### I3′ — Generation is non-decreasing along the log

> For each conversation, the sequence of `conversation_log.generation` ordered
> by `step` is non-decreasing.

This is precisely what fencing buys. If two owners both committed to one
conversation, the sequence interleaves and this fails loudly.

**Falsified by**

```sql
SELECT conversation_id, step, generation, prev_generation
  FROM (
    SELECT conversation_id, step, generation,
           lag(generation) OVER (PARTITION BY conversation_id ORDER BY step)
             AS prev_generation
      FROM conversation_log
     WHERE generation IS NOT NULL
  ) s
 WHERE prev_generation IS NOT NULL
   AND generation < prev_generation;
```

### I4 — Stale generations are rejected by the persistence layer

> A write carrying a generation other than the conversation's current
> generation is rejected, by the database, not by application bookkeeping.

**Falsified by** `TestFencedAppendRejectsStaleGeneration` — acquire, bump
generation from a second owner, then append with the first owner's generation
and require an error — and `TestFencedTransitionsRejectStaleGeneration` for
the ledger. `TestReplicas_KillHolderTransfersOwnership` does both from the
dead holder of a real takeover.

Under chaos the positive evidence is
`conductor_fenced_writes_rejected_total > 0`: if it is always zero during
forced failover, fencing is not actually being exercised and the invariant is
untested rather than satisfied.

### I5 — Steps are contiguous

> `conversation_log.step` runs from 1 with no gaps and no reordering, per
> conversation.

**Falsified by**

```sql
SELECT conversation_id, count(*) AS rows, max(step) AS max_step
  FROM conversation_log
 GROUP BY conversation_id
HAVING count(*) <> max(step);
```

### I6 — Rebuild is pure, for recoverable harnesses

> `rebuild(events)` is a pure function of the event sequence. For a
> `recovery: full` harness, post-recovery in-memory state equals the state
> rebuilt from the log alone.

**Scope is narrower than the original draft, deliberately.** For
`antigravityinteractions`, real state lives in (i) the remote Vertex
Interactions service, addressed by `previous_interaction_id`, and (ii) a local
file (`cursorstore.go:86`, under `cfg.StateDir`). `rebuild(events)` cannot
produce `prevInteractionID`, so for that harness post-recovery state is not
equal to rebuilt state and no wording can make it so.

**Known path to lifting this limit, not scheduled:** `StepEvent.interaction_id`
already exists in the published protocol (`proto/ax.proto:32`) and
`logger.interactionID` is declared (`controller.go:207`) and read
(`controller.go:258`, `:270`) — but **assigned nowhere in the repository**. It
is a dead field whose purpose is exactly to carry a remote cursor. Threading
`res.interactionID` from the harness into the log would eliminate the
local-file dependency and make I6 true for that harness. The obstacle is that
`Handler.OnMessage` has no slot for it and the `Harness` / `Handler` interfaces
are frozen (`ARCHITECTURE.md` §5.5). Spending interface-boundary budget on a
Google-only harness that will never be demonstrated is the wrong trade; the
note is the right one.

**Rebuild is additionally specified** to resolve duplicate results: where a
`call_id` has more than one `ToolResultStep` in the log — which happens after
repeated `IDEMPOTENT` retries — **the last one by `step` wins**. Without this
rule I6 is ambiguous.

**Falsified by** `TestRebuildIsPure` (same events ⇒ same state, repeated and
shuffled-batch) and `TestPostRecoveryStateEqualsRebuilt`.

### I7a — `runs` is a pure projection

> `runs` is derived entirely from `conversation_log`. Dropping and rebuilding it
> yields identical content.

**Falsified by** `TestRunsProjectionMatchesLog`
(`internal/controller/eventlog/runs_test.go`): the row the append path wrote
is diffed against the row derived from `conversation_log` alone by the
migration's backfill query, including across a retried append. The
reconciler's bookkeeping columns (`recovery_attempts`, `next_attempt_at`)
are not part of the projection.

Recovery uses `runs` **only to find candidates**. Truth is always re-derived by
`rebuild(events)`. A stale or wrong projection must degrade to "slower
discovery", never to "wrong decision".

### I7b — `tool_calls` is a log of record, not a projection

> `tool_calls` is authoritative state, not derived. Each row's **terminal
> state** agrees with the corresponding `ToolResultStep` in
> `conversation_log` — or with its absence, for `OUTCOME_UNKNOWN` and
> `ABANDONED`.

The original I7 demanded full reconstructibility of `tool_calls` from the log.
That is unsatisfiable without inventing a parallel event taxonomy:
`Step.type`'s oneof is `{ContentStep, ThoughtStep, ToolCallStep,
ToolResultStep}` (`proto/ax.proto:133-140`) and `ToolResultStep`
(`ax.proto:177-186`) carries `call_id`, `signature` and `function_result` —
no state, no attempt count. Weakening to agreement-on-terminal-state is both
cheaper and honest.

**Falsified by**

```sql
-- a settled-successful call with no result step in the log
SELECT t.call_id
  FROM tool_calls t
 WHERE t.state = 'SUCCEEDED'
   AND NOT EXISTS (
     SELECT 1 FROM conversation_log l
      WHERE l.conversation_id = t.conversation_id
        AND l.payload::jsonb @> jsonb_build_object(
              'steps', jsonb_build_array(
                jsonb_build_object('tool_result',
                  jsonb_build_object('call_id', t.call_id))))
   );
```

The payload is protojson with proto field names (`UseProtoNames`,
`eventlog/eventlog.go:42`), so the keys are `tool_result` / `call_id`; the
camelCase spelling would match nothing and report every call.

### I8 — Do not start what you cannot commit (Conductor-owned loops only)

> Conductor does not initiate an `UNSAFE` side effect **that it owns** when the
> remaining local monotonic lease deadline is less than
> `toolTimeout + margin`.

**Two narrowings relative to the original draft, both necessary.**

*Scope.* The check is evaluated in the controller, but for every upstream
harness the tool executes in another process — the Python sidecar, a remote
pod, or inside the harness itself
(`antigravityinteractions.go:408 e.harness.executeTool(ctx, call)`). The
controller has no veto point there; it only observes a `ToolCallStep` after the
fact, and today not even that (G1). Giving the controller a veto would require
a new `HarnessRequest` frame type, which is out of scope. **I8 is therefore
enforceable only in `recovery: full` harnesses.** For the others, the ledger's
`OUTCOME_UNKNOWN` path is the only mitigation, and the README says so.

*Clock.* "Remaining lease time" is a **local monotonic** quantity, derived from
the `ttl_remaining` the database returned, anchored at the moment the request
was *sent*:

```
deadline = requestStartMonotonic + ttlRemaining
```

so error always lands on the safe side. **A database `TIMESTAMPTZ` is never
compared against `time.Now()`, anywhere.** Replica clock skew then cannot
affect ownership: Postgres's clock alone decides who owns a conversation, and
each replica only decides, conservatively, when to stop.

**Falsified by** `TestUnsafeToolNotStartedNearLeaseExpiry` — shrink TTL below
the tool budget and require the call to stay `PLANNED`.

### I9 — Nothing reaches the client before it is durable

> No step is delivered to a client before its append has committed.

This invariant exists because upstream violates it. `controller.go:153-162`:

```go
logStep, err := a.logger.LogOutputs(ctx, []*proto.Step{step}, proto.State_STATE_PENDING)
if err != nil {
    slog.WarnContext(ctx, "Failed to log streamed message to event log", ...)
}
if step != nil {
    step.Index = logStep          // zero value when the append failed
}
return a.execHandler(&proto.CreateInteractionResponse{Outputs: []*proto.Step{step}})
```

The append failure is logged at warn level and the step is delivered anyway,
stamped with `Index = 0`. `cmd/ax/exec.go:350` reads `if lastStep != 0`, so the
client silently discards it and nobody learns that a step was never persisted.
`OnComplete` (`controller.go:175-187`) swallows its append error the same way.

A client that can observe a step absent from the log makes
"the execution log is complete" false, which is `PLAN.md` §8 selling point 2.
The fix is to return the error instead of warning, which requires I10 so that
the inevitable retry is safe.

**Falsified by** `TestAppendFailureIsNotDeliveredToClient` — inject an event-log
write failure and require the RPC to error with no output delivered.

### I10 — Append is idempotent under retry

> A retried append carrying the same `event_id` is a no-op that returns the
> existing step index.

`eventlog/sql.go:40,46` computes `COALESCE(MAX(step),0)+1` inside a
default-isolation transaction (READ COMMITTED under pgx). Fencing prevents
cross-generation races, but the classic ambiguous commit remains: one owner
retries an append that timed out yet actually committed. Without idempotency
the outcomes are "lose a step" (if the error is treated as fatal) or "duplicate
content at a new step index" — and **neither violates I5 as written**, so
nothing catches it.

Each `StepEvent` therefore carries a client-generated `event_id` (UUID) with a
unique index. The append is `INSERT ... ON CONFLICT (event_id) DO NOTHING
RETURNING step`, falling back to a read of the existing row.

**Falsified by** `TestAppendIdempotentUnderRetry` — append, simulate an
ambiguous commit, retry with the same `event_id`, require one row and the same
returned step.

---

## 2. Coverage matrix

Which invariants block (6) asserts, by failure class.

| | I1 | I1b | I2 | I3′ | I4 | I5 | I6 | I7a | I7b | I8 | I9 | I10 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `controller_kill_graceful` | ● | ● | ● | ● | | ● | ● | ● | ● | | | |
| `controller_kill_sigkill` | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | | |
| `harness_kill` | ● | ● | ● | | | ● | ● | | ● | | | |
| `db_unavailable_on_append` | ● | | | | | ● | | ● | | | ● | ● |
| `duplicate_concurrent_req` | | | ● | ● | ● | ● | | | | | | ● |
| `rolling_restart` | ● | ● | ● | ● | ● | ● | ● | ● | ● | ● | | |

Every cell is a named query or test from §1. An empty cell means the failure
class cannot exercise that invariant, not that it is unchecked elsewhere.

---

## 3. Things deliberately not guaranteed

- **Exactly-once external side effects.** Refused, not deferred (§0.1).
- **At most one running process per conversation.** Only one *committing*
  writer (§0.2).
- **Recovery for harnesses that own their tool loop.** Structurally impossible
  over the current protocol (§0.3). Such conversations are skipped and counted,
  not silently half-recovered.
- **Multi-replica correctness on SQLite.** `Lease.Acquire` returns
  `ErrUnsupported` unless `--single-process` is set, and `serve` refuses to
  start with `CONDUCTOR_REPLICAS > 1` on SQLite. A degraded lease that *looks*
  like fencing is worse than none: it invites a deployment and then a
  correctness bug filed against the project's central claim. Every invariant
  here is asserted on PostgreSQL only.
- **Bounded replay cost.** Replay is linear in history length until snapshots
  land (P1). `conductor_replay_events_total` exists to show the growth rather
  than hide it.
