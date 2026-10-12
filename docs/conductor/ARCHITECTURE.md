# Conductor — Architecture

> Target architecture and component contracts. Companion to `PLAN.md`
> (decisions, scope, gap audit), `PRD.md` (product surface, milestones) and
> `correctness.md` (**the authoritative invariant set**).
>
> Revision 2 — incorporates the design review. Changes from revision 1 are
> marked **[R2]** where they reverse an earlier statement.

Module: `github.com/simondxu/conductor` · Upstream: `google/ax` @ `b777313` (Apache-2.0)

---

## 1. Target architecture

```
                         ┌──────────────────────────────┐
  client (CLI / gRPC)    │ InteractionsService          │
  ──────────────────────►│  CreateInteraction (stream)  │
                         └──────────────┬───────────────┘
                                        │
                       ┌────────────────▼─────────────────┐
                       │ Controller  (N replicas)         │
                       │  ┌────────────────────────────┐  │
                       │  │ Lease (fenced ownership)    │ │ NEW
                       │  ├────────────────────────────┤  │
                       │  │ Reconciler                  │ │ NEW
                       │  │  discover · rebuild ·       │ │
                       │  │  resolve · requeue          │ │
                       │  ├────────────────────────────┤  │
                       │  │ EventLog (fenced append)    │ │ MODIFIED
                       │  │ ToolLedger                  │ │ NEW
                       │  └──────────────┬─────────────┘  │
                       └─────────┬───────┴────────────────┘
                                 │                    │
                    ┌────────────▼──────┐    ┌────────▼──────────┐
                    │ Executor          │    │ PostgreSQL        │
                    │  local            │NEW │  conversation_log │
                    │  pod (plain k8s)  │NEW │  runs             │NEW
                    └────────────┬──────┘    │  tool_calls       │NEW
                                 │           │  conversation_    │NEW
                    ┌────────────▼──────────┐│    leases         │
                    │ harness/remote        │└───────────────────┘
                    │  generic gRPC client  │NEW
                    └────────────┬──────────┘
                                 │  HarnessService
                    ┌────────────▼──────────┐
                    │ Harness               │
                    │  fake        NEW  full│  ← recovery: full
                    │  openaicompat NEW full│  ← recovery: full
                    │  antigravity*  kept none
                    │  substrate     kept none
                    └───────────┬───────────┘
                                │
                      model endpoint · tools / MCP
```

### 1.1 Disposition of upstream components

| Component | Disposition |
|---|---|
| `proto` (`InteractionsService`, `Step`, `HarnessService`) | **kept**; additive fields only (`event_id`) |
| `internal/server` | **modified** — `inFlight` demoted to a same-process fast path; its `FailedPrecondition` behaviour is now a documented reconnect contract (§6.3) |
| `internal/controller` | **modified** — fenced append, reconciler, lease lifecycle, I9 fix |
| `internal/controller/eventlog` | **modified** — `generation`, `event_id`, `EventsSince`, `runs` projection |
| `internal/config` | **modified [R2]** — not untouched: it hardcodes `defaultNamespace = "ax"` and `AntigravityHarnessTemplate` (`config.go:30-43`) and **imports `internal/harness/substrate` directly** (`config.go:25`) |
| `internal/harness` interfaces | **kept unchanged** — the model-neutrality boundary |
| `internal/harness/remote` | **new [R2]** — generic `HarnessService` client. Upstream duplicates this logic inside `substrateExecution` (`substrate.go:194-233`) and `antigravityExecution` (`antigravity.go:163-200`); only `harness.DrainStream` (`stream.go:27`) is shared. Every executor backend needs it. ~80 LOC, and it deletes duplication. |
| `antigravity`, `antigravityinteractions`, `substrate` | **kept, unwired for recovery** — `recovery: none` (§1.3) |
| `internal/skills`, `pythonsidecar` | path/prefix churn from the rename only |
| `lease`, `toolledger`, `reconciler`, `executor`, `harness/fake`, `harness/openaicompat` | **new** |

### 1.2 Model neutrality is architectural

The runtime contains **no provider code**. `HarnessService` is the boundary.
`openaicompat` takes `base_url` + `model` and therefore covers OpenAI, Ollama,
vLLM, llama.cpp, LM Studio, Together, Fireworks, Groq, OpenRouter, DeepSeek,
Mistral, and Gemini via its OpenAI-compatibility endpoint. Azure OpenAI needs
minor routing differences. Anthropic has no OpenAI-compatible endpoint and gets
a native adapter later, or users point at a proxy.

**Honest boundary:** chat completion is portable; **tool calling is not**. The
claim is "any OpenAI-compatible endpoint **with tool-calling support**", and the
README carries a table of combinations actually tested.

### 1.3 The recoverable-harness contract **[R2]**

Controller-level recovery only closes the loop when **Conductor owns the tool
loop**. Upstream's protocol forecloses the alternative: `HarnessRequest`'s oneof
is `{start, cancel}` (`proto/ax.proto:50-56`) and
`antigravityinteractions/server.go:143-151` states that carrying input after
`start` is intentionally unsupported. A tool reconciled to `SUCCEEDED` during
recovery cannot be delivered to a harness-internal model loop.

Harnesses therefore declare `recovery: full | none` in the registry. The
reconciler skips `none` and counts
`conductor_recovery_skipped_total{reason="harness_not_recoverable"}`.

**Consequence for scope: `openaicompat` is P0, not P1.** It is the only
shippable harness that both runs a real model and satisfies the contract.
Without it every recovery number in block (6) is measured against a harness this
project wrote itself.

Full statement in `correctness.md` §0.3.

### 1.4 The owned-loop turn protocol **[M1]**

Decided during M1, because §1.3 and `PRD.md` §6.5 require that "Conductor, not
the harness, decides when each tool runs" without saying how, and the
`Harness` / `Execution` / `Handler` interfaces and `HarnessService` are frozen.

A `recovery: full` harness is **stateless across `Run` calls** and performs
**one turn per `Run`**:

- The controller `Queue`s the **whole conversation history** (every logged
  step, then the new inputs) before each `Run`.
- If the history ends with `ToolCallStep`s that have no matching
  `ToolResultStep` (by `call_id`), the turn is a **tool turn**: execute exactly
  those calls and emit one `ToolResultStep` each.
- Otherwise it is a **model turn**: emit thought/content steps and zero or more
  `ToolCallStep`s.

The controller loops while the history ends in a tool call, a tool result, or
user input, and writes `STATE_COMPLETED` once, after the loop — not on each
turn's `OnComplete`.

Why this shape: the gap between two `Run` calls is the only point, under the
frozen interfaces, where the controller can act **before** a tool runs. It is
where the ledger's `PLANNED` → `STARTED` writes, the I8 budget check and the
fencing check go (M3/M7). It works unchanged in-process and over
`HarnessService` (each `Run` is one `Connect` whose `start` carries the
history), so tools still execute inside the execution environment and
`harness_kill` mid-tool stays meaningful. Recovery is the same loop with history
rebuilt from the log (§6.1).

A resumed conversation whose history ends in an unanswered tool call is
resolved from the ledger before the next turn (§6.1, "Resolving unanswered
calls"). Without a ledger (SQLite) it is refused rather than re-executed:
re-running an `UNSAFE` call blind would be exactly the duplicate effect the
project exists to prevent.

---

## 2. Data model

PostgreSQL is the only production backend. On SQLite, `Lease.Acquire` returns
`ErrUnsupported` unless `--single-process` is set, and `serve` refuses to start
with `CONDUCTOR_REPLICAS > 1` **[R2]** — a degraded lease that looks like
fencing is worse than none. Schema is versioned with `golang-migrate` under
`db/migrations/`.

### 2.1 Existing table, extended

```sql
-- upstream: conversation_log(conversation_id, step, payload)
ALTER TABLE conversation_log ADD COLUMN generation BIGINT;   -- NULL = pre-Conductor
ALTER TABLE conversation_log ADD COLUMN event_id    UUID;    -- NULL = pre-Conductor
CREATE UNIQUE INDEX conversation_log_event_id ON conversation_log (event_id)
  WHERE event_id IS NOT NULL;
```

`generation` records the ownership epoch that wrote the row (I3′).
`event_id` makes append idempotent under retry (I10).

**NULL semantics [R2]** — rows written by upstream `ax` before the fork have
both columns NULL. NULL `generation` is treated as generation 0, so the first
Conductor acquisition writes 1 and fencing works from there.
`conductor migrate up` **backfills `runs` from `conversation_log`**; without the
backfill every pre-existing conversation is invisible to the reconciler.

### 2.2 Ownership

```sql
CREATE TABLE conversation_leases (
    conversation_id TEXT        PRIMARY KEY,
    owner_id        TEXT,                    -- NULL when released
    generation      BIGINT      NOT NULL,
    acquired_at     TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX conversation_leases_expiry ON conversation_leases (expires_at);
```

**Rows are never deleted [R2].** `Release` sets `owner_id = NULL` and
`expires_at = now()`, preserving `generation`. If the row were deleted, the next
`Acquire` would restart at generation 1 and a stale writer still holding
generation 1 would be accepted — the fencing token must outlive the lease.
Terminal-run rows are tidied by the reconciler, which also bumps generation, so
monotonicity survives cleanup.

Acquisition is one statement with **no application clock** **[R2]**:

```sql
INSERT INTO conversation_leases AS l
       (conversation_id, owner_id, generation, acquired_at, expires_at)
VALUES ($1, $2, 1, now(), now() + $3)
ON CONFLICT (conversation_id) DO UPDATE
   SET owner_id    = $2,
       generation  = l.generation + 1,
       acquired_at = now(),
       expires_at  = now() + $3
 WHERE l.expires_at < now()
RETURNING generation, (expires_at - now()) AS ttl_remaining;
```

`Renew` is the same shape with
`WHERE owner_id = $2 AND generation = $4 AND expires_at > now()`.

Postgres's clock alone decides ownership. Replicas convert `ttl_remaining` into
a **local monotonic deadline anchored at request send time**, so error lands on
the safe side. A DB `TIMESTAMPTZ` is never compared to `time.Now()`.

### 2.3 Run index

```sql
CREATE TABLE runs (
    conversation_id   TEXT        PRIMARY KEY,
    state             TEXT        NOT NULL,   -- PENDING|COMPLETED|FAILED|CANCELED
    agent_id          TEXT,
    harness_recovery  TEXT        NOT NULL,   -- full|none  [R2]
    last_step         BIGINT      NOT NULL,
    generation        BIGINT,                 -- [R2]
    recovery_attempts INT         NOT NULL DEFAULT 0,  -- [R2] poison detection
    next_attempt_at   TIMESTAMPTZ,            -- [R2] backoff
    updated_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX runs_recoverable ON runs (state, next_attempt_at)
  WHERE state = 'PENDING' AND harness_recovery = 'full';
```

A **projection** of `conversation_log`, written in the same transaction as the
append so the reconciler can find candidates without scanning (G6). The UPSERT
is guarded **[R2]**:

```sql
... ON CONFLICT (conversation_id) DO UPDATE SET ...
    WHERE runs.last_step < excluded.last_step
```

without which a late or retried write pushes the projection backwards.

Recovery uses `runs` **only to discover candidates**. Truth is always re-derived
by `rebuild(events)` (I7a).

**As built [M4]** (migration 0005), two columns are dropped. `harness_recovery`
cannot be derived from the log, so keeping it would break I7a; the reconciler
passes this replica's `recovery: full` harness ids to the discovery query
instead. `generation` waits for fenced appends (M7). `recovery_attempts` and
`next_attempt_at` are the reconciler's bookkeeping, not projected columns;
I7a compares `state`, `agent_id` and `last_step`. The migration backfills
`runs` from the log with the same derivation the I7a test checks against.

### 2.4 Tool ledger

```sql
CREATE TABLE tool_calls (
    call_id            TEXT        PRIMARY KEY,
    conversation_id    TEXT        NOT NULL,
    step               BIGINT      NOT NULL,
    tool_name          TEXT        NOT NULL,
    args_hash          TEXT        NOT NULL,
    args               JSONB       NOT NULL,
    idempotency_key    TEXT        NOT NULL,
    class              TEXT        NOT NULL,  -- IDEMPOTENT|RECONCILABLE|UNSAFE
    state              TEXT        NOT NULL,  -- §3
    attempts           INT         NOT NULL DEFAULT 0,
    reconcile_deadline TIMESTAMPTZ,           -- [R2] bounds I1
    result             JSONB,
    error              TEXT,
    started_at         TIMESTAMPTZ,
    settled_at         TIMESTAMPTZ,
    generation         BIGINT      NOT NULL
);
CREATE INDEX tool_calls_conv      ON tool_calls (conversation_id, step);
CREATE INDEX tool_calls_unsettled ON tool_calls (state, reconcile_deadline)
  WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN');
```

**`call_id` is generated by Conductor, never taken from the provider:**

```
call_id = hex(sha256(conversation_id || step || tool_name || args_hash))[:32]
```

Provider tool-call IDs are not stable — OpenAI returns `tool_call_id`, local
runtimes often return nothing or a value that changes between retries. An
idempotency key must be recomputable from durable inputs on our side alone. The
multi-provider requirement forces the correct design here rather than weakening
it.

`step` is the call's position in the conversation's step sequence (the steps
of every logged event, in order), not the `conversation_log.step` row number:
the id is needed before the step is appended, and the position is equally
recomputable from the log. **[M3]**

`idempotency_key` defaults to `call_id`, stored separately so policy can diverge
without changing identity.

### 2.5 Tool classification

Declared by the tool definition, never inferred:

| class | meaning | recovery policy |
|---|---|---|
| `IDEMPOTENT` | read-only, or safe to repeat with the same key | re-execute with the same `idempotency_key`, `attempts++` |
| `RECONCILABLE` | not safely repeatable, but external state is queryable | query the external system, settle `SUCCEEDED`/`FAILED`; give up at `reconcile_deadline` |
| `UNSAFE` | neither repeatable nor queryable | `ABANDONED`, run fails; an operator may override via `inspect tool --settle` |

Three classes, three branches. No policy DSL, no effect-plugin framework.

---

## 3. Tool-call state machine

```
   PLANNED
      │  lease held · generation checked · I8 budget check (owned loops only)
      ▼
   STARTED ──────────────► SUCCEEDED        result observed
      │   │
      │   └──────────────► FAILED           deterministic failure, result observed
      │
      └──────────────────► OUTCOME_UNKNOWN  no response observed
                                │             (crash · timeout · lost lease)
                                │  durable resting state — the reconciler
                                │  picks it up again; never an in-memory
                                │  step between two function calls  [R2]
                                ├──► SUCCEEDED   reconciled: it happened
                                ├──► FAILED      reconciled: it did not
                                ├──► STARTED     retried (IDEMPOTENT), attempts++
                                └──► ABANDONED   UNSAFE policy, or
                                                 reconcile_deadline exceeded
```

**Settled:** `SUCCEEDED`, `FAILED`, `ABANDONED`. `OUTCOME_UNKNOWN` is not
settled.

Rules:

- Only the current lease holder writes a transition; every write carries
  `generation`.
- A run may not reach `COMPLETED` while any call is `STARTED` or
  `OUTCOME_UNKNOWN` (I1b).
- `attempts` increments on every `→ STARTED`, retries included.
- `OUTCOME_UNKNOWN` without resolution by `reconcile_deadline` becomes
  `ABANDONED` with `error = 'reconcile_deadline_exceeded'` **[R2]**, which is
  what makes I1 assertable in finite time.

**Never claim exactly-once external side effects.**

---

## 4. Correctness invariants

**See `correctness.md`** — the authoritative set (I1, I1b, I2, I3/I3′, I4, I5,
I6, I7a, I7b, I8, I9, I10), each with the query or test that falsifies it, plus
the three guarantees Conductor explicitly does **not** make.

Revision-2 changes: I3 demoted to a design property with I3′ as the assertion;
I6 and I8 scoped to `recovery: full` harnesses; I7 split into I7a/I7b; I9 and
I10 added; I1 bounded by `reconcile_deadline`.

---

## 5. Component contracts

Signatures are indicative; exact shapes settle during implementation.

### 5.1 Lease

```go
type Lease interface {
    // Acquire takes ownership, bumping the conversation's generation.
    // ErrHeld when a non-expired lease exists for another owner.
    // ErrUnsupported on SQLite without --single-process.
    Acquire(ctx context.Context, conversationID, ownerID string, ttl time.Duration) (Held, error)
}

type Held interface {
    Generation() int64
    // Remaining is a LOCAL MONOTONIC budget derived from the ttl_remaining the
    // database returned, anchored at request send time.  [R2]
    // Revision 1 exposed ExpiresAt() time.Time, which invited comparing a DB
    // timestamp against time.Now() — that was the clock-skew bug waiting to
    // happen.
    Remaining() time.Duration
    // Renew must be called with its own context deadline, strictly shorter
    // than Remaining(), or a stalled DB round-trip blocks the renewal loop and
    // Done() never fires while the lease silently expires.  [R2]
    Renew(ctx context.Context) error
    // Release preserves generation (§2.2); it does not delete the row.
    Release(ctx context.Context) error
    // Done closes when the lease is lost: renewal failed, the local deadline
    // elapsed, or a higher generation was observed. Execution must stop.
    Done() <-chan struct{}
}
```

### 5.2 EventLog

```go
type EventLog interface {
    // Append is fenced on gen, idempotent on ev.EventId, and updates the runs
    // projection in the same transaction. Callers must NOT swallow its error
    // (I9).
    Append(ctx context.Context, conversationID string, ev *proto.StepEvent, gen int64) (step int64, err error)
    Events(ctx context.Context, conversationID string) ([]*proto.StepEvent, error)
    EventsSince(ctx context.Context, conversationID string, afterStep int64) ([]*proto.StepEvent, error)
    // RecoverableCandidates returns PENDING runs with recovery:full whose
    // backoff has elapsed and whose lease is absent or expired.
    RecoverableCandidates(ctx context.Context, limit int) ([]RunRef, error)
}
```

### 5.3 ToolLedger

```go
type ToolLedger interface {
    Plan(ctx context.Context, gen int64, calls ...PlannedCall) error
    Start(ctx context.Context, gen int64, callID string) error
    Settle(ctx context.Context, gen int64, callID string, out Outcome) error
    MarkUnknown(ctx context.Context, gen int64, callID, reason string) error
    Unsettled(ctx context.Context, conversationID string) ([]ToolCall, error)
    // UnsettledAnywhere is the reconciler's SECOND discovery source: tool_calls
    // is authoritative (I7b) while runs is a projection that can be stale or
    // wrong, and these rows are the ones with real-world consequences.  [R2]
    UnsettledAnywhere(ctx context.Context, limit int) ([]ToolCall, error)
    // ExpireDeadlines moves OUTCOME_UNKNOWN past reconcile_deadline to
    // ABANDONED, which is what bounds I1.
    ExpireDeadlines(ctx context.Context) (int, error)
}
```

### 5.4 Executor

```go
// Executor owns the lifecycle of the execution environment, not the agent
// protocol. The dial target it returns is consumed by internal/harness/remote.
type Executor interface {
    Acquire(ctx context.Context, conversationID string, spec Spec) (Target, error)
    Release(ctx context.Context, conversationID string) error
}

type Target struct {
    Addr       string // host:port serving HarnessService
    Kind       string // "local" | "pod"
    Generation int64  // pods are labelled with it; see §6.2
}
```

Backends: `local` (subprocess) and `pod` (plain Kubernetes). **No Substrate
executor adapter [R2]** — it cannot be tested without a Substrate-equipped
cluster, and "kept for compatibility" is a maintenance promise this project
cannot honour. The `substrate` harness package stays in the tree, unwired, and
documented as unsupported; Apache-2.0 attribution is unaffected.

### 5.5 Harness — unchanged

`harness.Harness` / `Execution` / `Handler` stay exactly as upstream defines
them. Deliberate: it is both the model-neutrality boundary and the smallest
divergence surface. `antigravityinteractions` is modified only to **emit**
`ToolCallStep` and `ToolResultStep` through `Handler.OnMessage`, which
`Step.type` already supports — no protocol change (G1).

---

## 6. Recovery

### 6.1 Reconciler

One loop per replica: 5 s poll with jitter, bounded concurrency (4),
`FOR UPDATE SKIP LOCKED` so replicas do not pile onto the same head row. A
client reconnect calls the same `recoverOne(convID)` as a latency optimisation,
never a second code path.

**As built [M4]** (`internal/controller/reconciler.go`):

- **Sources A and B are one query**, `RecoveryCandidates`
  (`eventlog/runs.go`): PENDING `runs` ∪ conversations with a `STARTED` or
  `OUTCOME_UNKNOWN` call, restricted to this replica's `recovery: full`
  harnesses, with backoff elapsed and lease absent or expired. A call with no
  `runs` row has no backoff to record, so it is ordered last rather than
  starving the rest. `UnsettledAnywhere` is not a separate ledger method.
- **No `FOR UPDATE SKIP LOCKED`.** The lease is the lock: `Acquire` is one
  conditional statement, and the replica that loses gets `ErrHeld` and moves
  on.
- **`recoverOne` is `Exec` with no input.** The lease, `rebuild`, the
  terminal-state short circuit and `recoverCalls` are the same code a
  client's `--resume` runs. There is no separate recovery path.
- **Every `Exec` on PostgreSQL holds the conversation's lease**, not only the
  reconciler's. Otherwise the reconciler would take over a run a client is
  driving: its lease row is absent. A client that loses the race gets gRPC
  `FAILED_PRECONDITION`. Ledger writes carry the lease's generation. Appends
  are not fenced on it yet (M7).
- **Each pass runs `ExpireDeadlines` first**, which bounds I1.
- **Poison:** after `MaxAttempts` (5) recoveries, the run is failed by
  appending `STATE_FAILED` under its lease, never by writing `runs` directly.
  Its `STARTED` calls become `OUTCOME_UNKNOWN`, so their deadline still bounds
  them. `recovery_attempts` is never reset, so a run gets 5 recoveries in its
  lifetime. Backoff is 5 s doubling, capped at 5 min.
- **`serve` runs the loop on PostgreSQL only.** On SQLite it logs that
  ownership is process-local. `eventlog.postgres.lease_ttl` (default 30 s)
  sets how long a killed controller's runs wait; compose uses 5 s.

No `LISTEN/NOTIFY`: it adds a connection-state failure mode whose benefit is
unmeasurable, and a notification lost in the very crash under test would
silently weaken the guarantee.

**Two discovery sources [R2]**, because `runs` is a projection and `tool_calls`
is the record of real-world consequences:

```
A. RecoverableCandidates()   -- runs.state='PENDING', recovery:full,
                             -- backoff elapsed, lease absent/expired
B. UnsettledAnywhere()       -- tool_calls in STARTED|OUTCOME_UNKNOWN
                             -- catches runs rows that are stale, wrong, or
                             -- missing entirely
```

```
for ref in (A ∪ B), bounded concurrency:

  # poison guard BEFORE any parsing work, in its own committed txn  [R2]
  n ← bump runs.recovery_attempts(ref.ID)
  if n > maxRecoveryAttempts:            # 5
      runs.state ← FAILED, reason='recovery_poisoned'
      metric conductor_recovery_poisoned_total++
      continue
  set runs.next_attempt_at ← now() + backoff(n)

  held, err ← Lease.Acquire(ref.ID, ownerID, ttl)
  if err == ErrHeld:      continue                  # another replica won
  if err == ErrUnsupported: continue                # SQLite
  gen ← held.Generation()
  go renewLoop(held, ttl/3)                         # cancels execCtx on Done()

  events ← EventsSince(ref.ID, ref.SnapshotStep)
  st     ← rebuild(events)          # pure; last ToolResultStep per call_id wins

  # terminal-state short circuit  [R2] — without this the algorithm walks on to
  # harness.Start/Run with no input and fails forever on a self-healing case
  if st.State is terminal:
      repair runs projection; held.Release(); continue

  for tc in Unsettled(ref.ID):
      if tc.state == STARTED:
          MarkUnknown(gen, tc.call_id, "owner crashed in flight")
          # OUTCOME_UNKNOWN is durable: if we die here the next pass resumes
      switch tc.class:
          IDEMPOTENT:   Start(gen); re-execute with tc.idempotency_key; Settle
          RECONCILABLE: query external state; Settle(gen, observed)
                        or leave for a later pass until reconcile_deadline
          UNSAFE:       Settle(gen, ABANDONED); run ← FAILED

  exec ← harness.Start(execCtx, ref.ID, st.AgentConfig)
  exec.Queue(execCtx, st.PendingInputs...)          # the G3 fix
  exec.Run(execCtx, handler)
```

`rebuild` is pure over the event sequence, producing state, agent config,
pending input steps, and planned-but-unsettled calls. Where a `call_id` has
several `ToolResultStep`s — normal after repeated `IDEMPOTENT` retries — **the
last by `step` wins** **[R2]**; without that rule I6 is ambiguous.

**Resolving unanswered calls [M4].** `recoverOne`'s ledger step is
`recoverCalls` (`internal/controller/recover.go`), and a client `--resume`
runs the same code. Decisions made while building it:

- **A call answered in the log but still `STARTED` or `OUTCOME_UNKNOWN`** (the
  owner died between appending the result and settling it) is settled from
  the logged result, the last by `step` winning, and not run again. This runs
  on every resume of a `PENDING` run, not only when calls are unanswered:
  the window can fall on the last tool result before a model turn.
- **A call with no ledger row** (the owner died between logging the call and
  planning it) is planned under its logged `call_id` and run: it never
  started, so it cannot have had an effect.
- **`RECONCILABLE` with an unknown outcome stops the resume** with an error
  and stays `OUTCOME_UNKNOWN`. No query mechanism exists yet; the loop's
  `ExpireDeadlines` abandons it at `reconcile_deadline`.
- **When the run fails** (any `UNSAFE` call with an unknown outcome), every
  unanswered call of the turn is closed: unknown ones are settled
  `ABANDONED`, and each unanswered call gets an error `ToolResultStep`,
  written **in the same event as `STATE_FAILED`**. Without the result, the
  history would keep a call no later turn may answer, and an OpenAI-compatible
  endpoint rejects a tool call with no tool message; written separately, a
  crash between the two would leave the run answered but `PENDING`, and the
  next pass would run a model turn instead of failing. I7b holds: the
  falsifying query checks only `SUCCEEDED`.
- **New input over unanswered calls is refused** ("resume it first"), so a
  tool result is never logged after a later user message.
- **A step joins the turn's history only after its append succeeds, and
  before it is delivered**, so a result that never became durable cannot
  settle its call `SUCCEEDED` (I7b), and a durable one still settles when the
  client has gone away.

### 6.2 Losing the lease mid-execution

The real case: the controller is executing, its lease expires (GC pause,
partition, slow DB), another replica takes over at generation+1, and the
original process is still running.

1. **Renewal goroutine.** Renews at `ttl/3`, each call with its own context
   deadline shorter than `Remaining()`. The loop closes `Done()` when the local
   monotonic deadline elapses **whether or not a renew is in flight** **[R2]**,
   and cancels the execution context.
2. **Fencing is the backstop.** Any append or ledger write that races through is
   rejected on stale generation (I4), counted by
   `conductor_fenced_writes_rejected_total`.
3. **Do not start what you cannot commit.** For `recovery: full` harnesses only,
   an `UNSAFE` call requires `Remaining() > toolTimeout + margin` (I8). For
   harness-internal loops the controller has no veto point; `OUTCOME_UNKNOWN` is
   the only mitigation and `correctness.md` says so.
4. **Pod deletion does not fence effects [R2].** `DELETE` returns, the pod
   enters `Terminating`, and the container keeps running for
   `terminationGracePeriodSeconds` — possibly still inside `executeTool`. Only
   the ledger write fences. The ordering is therefore: **bump generation**
   (fences all writes) → delete lower-generation pods (best effort, resource
   reclamation) → **wait for disappearance or grace-period expiry** → `Acquire`
   a new environment for that conversation.

The guarantee is **at most one *committing* writer**, not one *running
process*. Stated as a named caveat beside the exactly-once prohibition
(`correctness.md` §0.2).

### 6.3 Reconnect: the `inFlight` behaviour is a user-visible contract **[R2]**

`internal/server/server.go:62-65` returns
`FailedPrecondition "conversation %q is already in flight"`. When a client
disconnects and immediately retries, the first `Exec` may still be unwinding —
context cancellation propagates asynchronously — so the retry is rejected.

Demoting the map to "an optimisation" does not remove this. Either the error
stays and `PRD.md` documents it as the reconnect contract with a retry
recommendation, or it becomes wait-or-takeover. It must not be left
undocumented.

---

## 7. Observability

### 7.1 Spans, with honest scope **[R2]**

`exec` → `lease.acquire` → `reconciler.replay` → `executor.acquire` →
`harness.turn` → `model.call` → `tool.execute` → `eventlog.append`

- **`fake` and `openaicompat`: the full chain.** Conductor owns the tool loop,
  so `model.call` and `tool.execute` are its own spans.
- **Upstream harnesses: partial chain only.** Those steps execute in another
  process — the Python sidecar, a remote pod, or a remote Google service.
  `otelgrpc.NewClientHandler()` on `Connect` (`substrate.go:77`,
  `antigravity.go:~158`) propagates context, so harness-side spans attach *if
  the harness is instrumented*. The Python sidecar is not.

Revision 1 claimed the whole chain unconditionally. It does not hold.

Upstream already configures a generic OTLP exporter (`serve.go:65-76`); this
work adds domain spans, not the pipeline. **Change the default endpoint
[R2]** — `defaultEndpoint = "telemetry.googleapis.com"`
(`cmd/ax/serve_gcp.go:22`, `internal/telemetry/telemetry.go:35`) means an
out-of-the-box Conductor points its traces at Google. Default to the local
collector instead.

### 7.2 Metrics

Every metric answers a stated operational question.

| Metric | Type | Labels | Question |
|---|---|---|---|
| `conductor_runs_active` | gauge | owner | How much work does this replica own? |
| `conductor_runs_total` | counter | outcome | Completion mix. |
| `conductor_run_duration_seconds` | histogram | outcome | End-to-end latency. |
| `conductor_recovery_total` | counter | result | Does recovery succeed? |
| `conductor_recovery_duration_seconds` | histogram | kill_kind | Stall after graceful vs ungraceful loss. |
| `conductor_recovery_skipped_total` | counter | reason | How much work is unrecoverable by contract (§1.3)? |
| `conductor_recovery_poisoned_total` | counter | — | Runs that crash their recovery repeatedly. **Without this, "recovery success rate" is dishonest** — a crash loop reports as "not yet recovered". |
| `conductor_replay_events_total` | counter | — | Replay cost growth; motivates snapshots. |
| `conductor_lease_acquisitions_total` | counter | result | Failover rate and contention. |
| `conductor_lease_losses_total` | counter | reason | `renew_timeout` · `deadline_elapsed` · `preempted` — real tuning data for TTL. |
| `conductor_fenced_writes_rejected_total` | counter | — | **Direct evidence for I4.** |
| `conductor_tool_calls_total` | counter | tool, class, state | Ledger distribution. |
| `conductor_tool_attempts_total` | counter | tool, class | Retry pressure. |
| `conductor_tool_duration_seconds` | histogram | tool | Tool latency. |
| `conductor_tool_outcome_unknown_total` | counter | tool, resolution | How often is the outcome genuinely unknown, and how is it resolved? |
| `conductor_duplicate_effects_total` | counter | tool | **Direct evidence for I2.** |
| `conductor_model_calls_total` | counter | provider, model | Model mix. |
| `conductor_model_duration_seconds` | histogram | provider, model | Model latency. |
| `conductor_model_tokens_total` | counter | provider, model, kind | Token use — **absent for local models; must degrade gracefully, never assume `usage` is present**. |
| `conductor_executor_acquire_duration_seconds` | histogram | kind | Cold-start cost per backend. |

**As built [M5].** Emitted and scraped in the compose stack (`smoke.sh`
checks for them by name):

- From the controller: every row above except the three below.
- From the `fake` harness process: `conductor_duplicate_effects_total`, on
  its own `--metrics-addr`, scraped as job `fake-harness`. Only the fake can
  see an external effect.

Not emitted yet:

- `conductor_fenced_writes_rejected_total` waits for fenced writes (M7).
- `conductor_executor_acquire_duration_seconds` waits for the executor (M6).
- `conductor_model_*` waits for `openaicompat`. Only it knows a provider and
  a model, and `fake` would give them a fake label.

Decisions made while building it:

- **`conductor_tool_calls_total` counts transitions, by the state entered.**
  A counter cannot say "unsettled right now", so
  `conductor_tool_calls_unsettled{state}` is a gauge that the reconciler
  samples each pass from the `tool_calls_unsettled` index. Every replica
  reports the same database-wide value: aggregate it with `max`.
- **Ledger metrics are counted from the transition the database made.** Each
  `UPDATE` returns the state it left (the row is locked first), so a no-op
  or refused write counts nothing.
  - `tool_duration_seconds` is `settled_at − started_at` by the database
    clock, for results observed live only.
  - `tool_outcome_unknown_total{resolution}` takes `retried`, `reconciled`
    (settled from a later-observed result), `abandoned` or
    `deadline_exceeded`.
- **`recovery_total{result}`** is `success | failure | held`, where `held`
  means another owner won the lease.
- **`recovery_duration_seconds` is the stall**: the run's idle time when the
  reconciler found it (database clock), plus the recovery itself.
  `kill_kind` comes from the lease row: `graceful` (released), `ungraceful`
  (expired) or `none`.
- **`recovery_skipped_total{reason="harness_not_recoverable"}`** counts each
  abandoned `recovery: none` run at most once an hour.
- **`lease_losses_total{reason}`**: `preempted` (the database refused a
  renewal), `renew_timeout` (a renewal was stuck or failing when the deadline
  passed), `deadline_elapsed` (otherwise).
- **`runs_active{owner}`** uses the owner id, `local` on SQLite.

**Logs [M5].** `serve` logs JSON through `telemetry.LogHandler`. It adds the
attributes a context carries (`conversation_id`, `owner_id`, `generation`,
set once per `Exec`) and the span's `trace_id` to every record. The recovery
path logs `recovery.start`, `recovery.rebuild`, `recovery.settle` and
`recovery.complete` (or `recovery.poisoned`), with counts.

---

## 8. Chaos and measurement

### 8.1 Six failure classes **[R2]**

Reduced from eight. Each maps to named invariants in `correctness.md` §2; the
dropped ones were duplicates — "slow downstream" and "model/tool timeout" are
one injection with different sleeps, and "client disconnect" belongs with the
P1 stream-replay work.

```
controller_kill_graceful    SIGTERM → Release() → fast handover
controller_kill_sigkill     SIGKILL at a named boundary → TTL expiry path
harness_kill                executor process / pod killed mid-tool
db_unavailable_on_append    append fails → exercises I9 and I10
duplicate_concurrent_req    two clients, one conversation
rolling_restart             replicas replaced under load
```

Graceful and ungraceful controller loss are **reported separately**. Graceful
handover should be sub-second because `Release()` is explicit; ungraceful pays
the TTL. One blended number hides the design.

Named kill boundaries, selected by `CONDUCTOR_CHAOS_KILL_AT`:

```
model_started · model_completed · tool_planned · tool_started ·
tool_effect_done · tool_settled · event_appended · lease_renewed
```

The hook aborts the process (`os.Exit`) rather than unwinding, so the failure
resembles a real kill. Determinism comes from `fake`.

### 8.2 Detecting duplicate effects mechanically

`fake` appends every side effect to `fake_effects`; a duplicate is more than one
row per `idempotency_key` for an `IDEMPOTENT` tool. I2 becomes a SQL assertion,
not a judgement. Schema and query in `correctness.md` §1 (I2). The table is test
fixture, never runtime.

---

## 9. Repository layout

```
cmd/
  conductor/              RENAMED from cmd/ax when the first new command lands
internal/
  controller/             MODIFIED  fenced append, I9 fix, lease lifecycle
    eventlog/             MODIFIED  generation, event_id, EventsSince, projection
    lease/                NEW
    toolledger/           NEW
    reconciler/           NEW
  executor/               NEW       local/ · pod/        (no substrate adapter)
  harness/
    harness.go            UNCHANGED (the neutrality boundary)
    remote/               NEW       generic HarnessService client  [R2]
    fake/                 NEW       deterministic, zero-key, recovery: full
    openaicompat/         NEW       base_url + model, recovery: full
    antigravity/          upstream, recovery: none
    antigravityinteractions/  MODIFIED  emit tool lifecycle (G1), recovery: none
    substrate/            upstream, unwired, documented unsupported
  server/                 MODIFIED  inFlight → fast path; reconnect contract
  config/                 MODIFIED  ax-prefixed defaults; substrate coupling
  telemetry/              MODIFIED  metrics pipeline; non-Google default endpoint
db/migrations/            NEW       golang-migrate; backfills runs
deploy/compose/           NEW       one-command local stack
deploy/k8s/               NEW       plain manifests, no Agent Substrate
bench/                    NEW       chaos + load harness
docs/conductor/           PLAN · ARCHITECTURE · PRD · correctness
```

---

## 10. Resolved design decisions

Revision 1 listed these as open. All six are now decided.

| | Decision | Rationale |
|---|---|---|
| **TTL / renewal** | **30 s TTL, renew at 10 s.** Graceful paths (SIGTERM, rolling update, `kubectl delete` with grace) call `Release()` explicitly. | 15 s/5 s leaves only two missed renewals of headroom against a GC pause plus a stalled DB round-trip. The 30 s worst-case stall is avoided by releasing on the graceful path rather than by shortening the TTL, so planned handover is sub-second and only SIGKILL or node loss pays it. TTL is configurable, and `conductor_lease_losses_total{reason}` supplies the real tuning data. |
| **Where recovery runs** | **A reconciler loop per replica**, plus `recoverOne` on client reconnect as a latency optimisation. | Only a loop supports "a session with no client attached still finishes", which is the claim worth making — and blocks (3) and (4) assert exactly that. |
| **`runs` projection** | **Projection, written transactionally; never authoritative in recovery.** | Keeps I7a a cheap test and the hot path one round-trip. An async projector would create a lag window during which the reconciler cannot see a PENDING run — precisely the window chaos attacks. |
| **`UNSAFE` default** | **Fail the run**, `ABANDONED`, with the nature of the uncertainty in `error`. Plus an operator write path: `conductor inspect tool --settle <call_id> --outcome ... --reason ...`. | Parking requires durable HITL, which is out of scope, and an unresolvable parked state is an unbounded leak that makes I1 unfalsifiable. The ~30-line CLI write path turns "manual resolution" from aspiration into a real mechanism while `ABANDONED` stays settled. |
| **SQLite** | **No lease at all.** `Acquire` returns `ErrUnsupported` unless `--single-process`; `serve` refuses `CONDUCTOR_REPLICAS > 1`; `doctor` reports `ownership=process-local`. Every invariant is asserted on PostgreSQL only. | A degraded lease that looks like fencing invites a deployment and then a correctness bug filed against the central claim. SQLite's only job is the zero-dependency local demo. |
| **Binary rename** | **At the first new command (`inspect`)**, as one mechanical commit: `cmd/ax` → `cmd/conductor`, flag/env prefixes, `conductor.yaml` (still accepting `ax.yaml` for one release). No `ax` alias. | Earlier and every doc fragment is written once; later and README, compose, manifests, bench scripts and PRD all need rewriting. The current state — module renamed but binary, config name, `AX_HARNESS_WORKDIR` (`antigravityinteractions.go:381`), `config.AXAssetsDir()` and `defaultNamespace = "ax"` still `ax` — is the worst of both. |
