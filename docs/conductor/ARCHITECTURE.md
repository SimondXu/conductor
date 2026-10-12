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

**As built [P1]: gVisor.**

- **Configuration.** `registry.fake.pod.runtime_class` becomes the harness
  pods' `runtimeClassName`. `deploy/k8s/gvisor.yaml` is the `RuntimeClass`
  `gvisor`, with handler `runsc`. Nothing else changes: the pod lifecycle,
  fencing and recovery are the same.
- **The e2e.** `GVISOR=1 deploy/k8s/kind-e2e.sh`:
  1. Installs `runsc` and its containerd shim on the kind node, from gVisor's
     release bucket, sha512-checked.
  2. Registers the `runsc` handler with containerd.
  3. Runs the M6 assertion (delete the harness pod mid-tool-call; the run
     recovers and completes) with `runtime_class: gvisor`.
  4. Before deleting the pod, requires that it has `runtimeClassName:
     gvisor` and that its kernel is gVisor's, by its `dmesg` or its fixed
     `/proc/version`.
- **Where it is checked.** The CI job `pod-executor-gvisor` runs the e2e.
- **What isolation means here.** It is the runtime's, not Conductor's: a
  tool still runs with the harness's privileges, inside the sandbox
  (PRD §10).

**As built [M6]** (`internal/executor`):

- **`executor.Harness` adapts an `Executor` to `harness.Harness`.** `Start`
  acquires an environment and dials it with `internal/harness/remote`;
  closing the execution releases it. So there is one environment per `Exec`,
  that is, per ownership generation. `Start` is frozen, so the generation
  reaches `Acquire` through the context (`executor.WithGeneration`, set by
  the controller next to its lease).
- **`Release(ctx, conversationID, gen)` takes the generation**, unlike the
  signature above. A stale owner's late release must not remove the newer
  owner's environment.
- **`local`** runs `conductor harness fake --port 0` per conversation and
  reads the address from its log. A newer acquisition replaces the older
  subprocess. On Linux the child gets `Pdeathsig`, so a SIGKILLed controller
  leaves no orphans. Config: `registry.fake.executor: local`.
- **`pod`** (in-cluster only: the controller dials pod IPs) uses the core/v1
  typed client, not the full clientset, which made the binary 12 MB larger
  on its own. client-go still grows it from 41 MB to 76 MB.
  - **Pods**: one per conversation and generation, named
    `harness-<hash>-g<gen>`. Labels: the conversation hash, the generation,
    and `app.kubernetes.io/component=harness`. The conversation id goes in
    an annotation.
  - **Spec**: `restartPolicy: Never`, because a dead harness is replaced by
    the reconciler at the next generation, never restarted in place. A gRPC
    readiness probe gates readiness.
  - **`Acquire`** deletes the conversation's pods of other generations and
    waits until they are gone or their grace period is over, then creates
    its own and watches it until ready. Fencing stays with the ledger: §6.2.
  - **Config**: `registry.fake.executor: pod` with `pod.image`,
    `pod.secret` (env for the harness, e.g. `CONDUCTOR_DSN`) and
    `pod.grace_period_seconds` (default 5).
  - **Manifests**: `deploy/k8s/conductor.yaml` holds PostgreSQL, the
    controller, and a Role limited to pods in its namespace.
- **DoD**: `deploy/k8s/kind-e2e.sh`, also run in CI as the `Kind` workflow.
  It deletes the harness pod while the call is STARTED, and the reconciler
  completes the run in a new pod at the next generation (`SUCCEEDED 2`).

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

**As built [P1]: replay snapshots** (migration 0007, PLAN.md G6).

- **What is stored.** `conversation_snapshots` holds one row per
  conversation: the events `1..step` folded into a single `StepEvent` (the
  first harness id, the last agent_config and state, every step in order),
  as binary protobuf.
- **How replay uses it.** `replay` reads the snapshot and then
  `EventsSince(step)`. When that tail reaches 100 events, it saves a new
  snapshot at the end of the tail. `conductor_replay_events_total` now
  counts only tails.
- **Why it needs no fencing.** The log is append-only, so the fold of its
  first `step` events is the same whoever computes it. The upsert keeps the
  highest step.
- **What happens when a snapshot fails.** A snapshot is a cache: one that
  cannot be read or saved costs a full replay, nothing worse.
- **No compaction.** Old log rows are not deleted. I5, I7a and I7b,
  `Attach` catch-up and `inspect` all read the full log, and it is the
  audit record. Deleting old rows would be a retention policy, which is a
  separate decision.
- **The honest limit.** An owned-loop harness gets the whole history every
  turn, so a snapshot cuts rows read and per-row JSON decoding, not history
  size. `BenchmarkReplay` on a laptop:

  | log | whole log | snapshot + 10-event tail |
  |---|---|---|
  | 2000 events | 7.5 ms | 3.0 ms |
  | 200 events | 1.3 ms | 0.8 ms |

  Making history itself bounded would need summarisation, which is model
  behaviour and out of scope (PLAN.md §9).

**As built [M4]** (`internal/controller/reconciler.go`):

- **Sources A and B are one query**, `RecoveryCandidates`
  (`eventlog/runs.go`): PENDING `runs` ∪ conversations with a `STARTED` or
  `OUTCOME_UNKNOWN` call, restricted to this replica's `recovery: full`
  harnesses, with backoff elapsed and lease absent or expired. A call with no
  `runs` row is not a candidate: `runs` is written with the log, so its
  conversation has no log to recover from. `UnsettledAnywhere` is not a
  separate ledger method.
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

**As built [M7].**

- **The fence is a shared lock on the lease row, in the write's own
  transaction** (`lease.CheckFence`). Every `Append` (which now also records
  `conversation_log.generation`, migration 0006) and every ledger write
  (`Plan`, and each transition after it locks the call row) first runs
  `SELECT generation … FOR SHARE`. It then fails with `lease.ErrFenced`
  unless the generation matches, and counts
  `conductor_fenced_writes_rejected_total`. A takeover's `UPDATE` waits for
  that lock, so no write at the old generation can commit after a newer
  generation exists. The holder whose lease merely expired is still the
  current generation until someone takes over, and its writes are accepted;
  that is I4 as stated.
- **Generation 0 is accepted only for a conversation that has no lease
  row** (SQLite, or a database used without leases). Otherwise an unleased
  writer could bypass the fence.
- **`ExpireDeadlines` is not fenced.** It is a database-wide janitor with no
  generation, and it only moves `OUTCOME_UNKNOWN` calls past their deadline,
  which no owner may still resolve.
- **A fenced append is not retried.** It fails the same way every time.
- **SIGTERM (`serve`):** stop the reconciler, give in-flight RPCs
  `--drain-timeout` (default 20 s) to finish, then cancel them. Each `Exec`
  releases its lease as it unwinds, and `serve` waits for every `Exec`
  (`Controller.Wait`) before closing its pools. `server.GracefulStop` no
  longer closes the controller first; the upstream code did, which would
  have failed the very writes and releases a drain exists for.
- **Compose runs three replicas.** Only the first publishes a host port.
  `smoke.sh` SIGKILLs the owner mid-tool-call, and a surviving replica
  completes the run after the lease ttl. It also SIGTERMs the owner, and a
  replica takes over without waiting out the lease (`kill_kind="graceful"`).
  Each run ends once (I3′ and I1b are checked).

**As built [M9]: I8.** Before a tool turn that contains an `UNSAFE` call, the
controller requires `Remaining()` to exceed the budget, renewing once first
if it does not. There is no tool timeout yet, so the budget is the renewal
period, `ttl/3`. A refused turn starts nothing: its calls stay `PLANNED`, the
run stays `PENDING`, and the reconciler runs them later under a fresh lease.
Each decision is counted in
`conductor_unsafe_tool_turns_total{decision="allowed|deferred"}`.
`TestUnsafeToolNotStartedNearLeaseExpiry` falsifies it.

### 6.3 Reconnect: the `inFlight` behaviour is a user-visible contract **[R2]**

`internal/server/server.go:62-65` returns
`FailedPrecondition "conversation %q is already in flight"`. When a client
disconnects and immediately retries, the first `Exec` may still be unwinding —
context cancellation propagates asynchronously — so the retry is rejected.

Demoting the map to "an optimisation" does not remove this. Either the error
stays and `PRD.md` documents it as the reconnect contract with a retry
recommendation, or it becomes wait-or-takeover. It must not be left
undocumented.

**As built [P1]: `ConductorService.Attach`.** A reconnect no longer goes
through the guard. `proto/conductor.proto` adds Conductor's own service
beside the inherited one: `ax.proto` follows the pinned upstream and is not
extended, and #320's `last_seq` is not revived. `Attach(conversation_id,
after_step)` works like this:

- **What it streams.** Every logged step after `after_step`, one per
  response with `Step.index` set to its log step. Then it polls
  `EventsSince` every 100 ms and sends each new step, and ends when the run
  is `COMPLETED`, `FAILED` or `CANCELED`.
- **Where it runs.** It reads only the log, so every step it sends is
  durable (I9), and any replica serves it, wherever the run executes. It
  takes no in-flight slot, so a client that reconnects while its old turn
  unwinds is not refused.
- **Who drives the run.** While the run is `PENDING`, Attach resumes it
  itself, at most once and under the in-flight guard: SQLite has no
  reconciler, and on PostgreSQL this saves waiting for one. While someone
  else drives the run, so its lease is held or the conversation is in
  flight here, it may try again a second later. Once its own resume has run,
  successful or not, retrying belongs to the reconciler and its poison
  guard.
- **Granularity.** The unit is the log event. An event with several steps
  (the user inputs, a failed run's error results) is sent whole.
- **Client.** `conductor run --server … --conversation <id> --resume
  --after-step N` uses Attach. A broken stream prints that command with
  the last step received.
- **Checked.** `TestAttach_ReconnectGetsEveryMissedStepOnce`: through a real
  gRPC server on PostgreSQL, a client drops after the first tool call, and
  a second one attached at that step receives exactly the log's later
  steps, in order, until `COMPLETED`. With the boundary off by one, it
  fails on a duplicate. On compose, a `kill -9`ed CLI client re-attached
  on another replica and got the rest of the run while a third replica
  finished it.

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

**As built [M8].**

- **Spans:**
  - `exec` (one per `Exec`) has three children: `lease.acquire` (with the
    generation), `replay` (with the event count) and `harness.turn` (`kind`
    is `model` or `tool`).
  - A model turn has a `model.call` child that wraps the turn's `Run`.
  - A tool turn has one `tool.execute` per call, open from the call's start
    until its result is logged, or ended with an error status if no result
    came. A tool turn's `Run` is one RPC for every call, so the RPC span is
    `tool.execute`'s sibling, not its child.
  - `executor.acquire` comes from `executor.Harness`, `eventlog.append` and
    `eventlog.events` from the log, and `recovery` is a new root for each
    reconciler recovery.
- **Every process exports.**
  - `serve` reads `telemetry.otlp` from its config. Its default endpoint is
    now `localhost:4317`; Google Cloud Trace is used only when it is
    configured explicitly.
  - The CLI client (`conductor.run`, the root span) and `conductor harness
    fake` export when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
  - gRPC client and server stats handlers carry the context across
    processes.
  - Local executor subprocesses inherit the environment, so they export too.
- **The DoD is checked twice.**
  - `TestTrace_OneTraceFromClientToAppend` drives a real gRPC server and a
    local executor subprocess on Postgres. One trace holds the client span,
    the server span, `exec`, `lease.acquire`, `replay`, `executor.acquire`,
    `harness.turn`, `model.call`, `tool.execute` and `eventlog.append`.
  - The compose stack exports to Jaeger 2 (UI on `:16686`, a Grafana data
    source). `smoke.sh` finds the client's trace for a run and requires
    spans from `conductor-cli`, `conductor-server` and `fake-harness`, with
    every hop. Compose dials a fixed harness, so its traces have no
    `executor.acquire`.
- **Dashboard:** `conductor-runtime` has one panel per PRD §4.2 question
    (15).
  - "How long did the last recovery take, and how many tool attempts did it
    involve" reads `conductor_last_recovery_{duration_seconds,tool_attempts}`
    from the replica with the newest `conductor_last_recovery_timestamp_seconds`.
    A histogram cannot give the last value.
  - The Model latency panel waits for `conductor_model_*`.

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
| `conductor_unsafe_tool_turns_total` | counter | decision | How often does I8 hold back an `UNSAFE` call? |
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

- From the controller: every row above except the ones below.
- From the `fake` harness process: `conductor_duplicate_effects_total`, on
  its own `--metrics-addr`, scraped as job `fake-harness`. Only the fake can
  see an external effect.
- Since M7: `conductor_fenced_writes_rejected_total`.
- Since M9: `conductor_unsafe_tool_turns_total`.
- Since M6: `conductor_executor_acquire_duration_seconds`. It is emitted only
  where an executor is configured; the compose stack dials a fixed harness,
  so it does not have it.

Not emitted yet:

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
- **`recovery_total{result}`** is `success | failure | held | canceled`,
  where `held` means another owner won the lease and `canceled` that this
  replica shut down mid-recovery. Neither counts against the poison guard:
  the attempt is refunded and its backoff cleared.
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

**As built [M9] (`internal/chaos`).** `CONDUCTOR_CHAOS_KILL_AT=<boundary>[:<tool>]`
is read at startup; an unknown boundary, or a tool name on a boundary that is
not `tool_*`, panics. The process exits with status 137 at the first match,
after logging `chaos.kill`. Where each fires:

- `model_started` / `model_completed`: around a model turn's `Run`, in the
  controller. At `model_completed` the turn's calls are logged, not planned.
- `tool_planned`, `tool_started`, `tool_settled`: right after the ledger
  write, once per call, matched against the call's tool.
- `tool_effect_done`: in the `fake` harness, after the first effect for a
  key is recorded and before the result is returned. It does not fire on a
  retry, so a harness that restarts armed does not crash-loop.
- `event_appended`: after every successful append, the first being the
  user input.
- `lease_renewed`: after a successful renewal in `lease.KeepAlive`.

`TestChaosBoundaries` checks what the ledger and the effects show at each
point.

**As built [M9] (`conductor chaos run`, `internal/bench`).** It drives the
compose stack from the host: containers through the docker compose CLI, runs
over gRPC to each replica's published port (8494–8496), outcomes and
invariants in PostgreSQL. Each injection is a new conversation running a
fake script whose `IDEMPOTENT` charge takes 4 s, longer than the 2 s drain
and than a renewal period of the stack's 5 s lease; then an `UNSAFE` email.
After each repeat, `internal/invariant` checks that repeat's conversations.

| Class | Injection | Measured from |
|---|---|---|
| `controller_kill_sigkill` | `conductor` is recreated with `CONDUCTOR_CHAOS_KILL_AT` (default `tool_started:charge_card`) and dies there; it is started again, still armed, after each run, and disarmed at the end | the container exiting |
| `controller_kill_graceful` | SIGTERM to the owner while the charge runs | the signal |
| `harness_kill` | the script's `crash_after_effect`; compose restarts the harness | the client's stream breaking |
| `db_unavailable_on_append` | `docker compose pause postgres` for 6 s while the charge runs, longer than the lease, so the result append and every renewal block and the owner loses the lease. A pause, not a stop: connections hang as on a partition instead of failing fast | the pause |
| `duplicate_concurrent_req` | the same conversation sent to two replicas at once; exactly one client may succeed | the send |
| `rolling_restart` | each replica in turn gets a run, then `docker compose restart` once its charge has started | its restart |

"Recovered" is the run leaving `PENDING` within 90 s; `FAILED` counts, since
an `UNSAFE` call cut off mid-flight fails the run by design.

Measured on a laptop: SIGKILL recovers in 4.5–12.5 s across all seven
controller boundaries (one injection each).

**Graceful handover.**
- First measurement: 6.5–10.4 s. The released run waited for another
  replica's next reconciler pass, a 5 s interval plus jitter.
- Since then, an `Exec` whose caller went away (a drain, a client
  disconnect) sends `NOTIFY conductor_handover` after its `Release`. Every
  replica's reconciler `LISTEN`s and starts a pass at once.
- A recovery that loses the lease (`held`) or is cut off by its own
  replica's shutdown (`canceled`) is refunded. Before this it set a backoff,
  and the next owner of a run handed over twice had to wait it out (one
  rolling restart took 16 s).
- Now the takeover starts about 0.1 s after the release. A graceful
  injection takes a steady 6.2 s: the 2 s drain, plus re-running the 4 s
  charge, plus the email. The rolling restart takes 6.1–8.8 s; the upper
  end is a run handed to a replica that is restarted next.
- `TestReconciler_TakesOverHandoverAtOnce` holds the takeover under 5 s
  with a one-minute reconciler interval.

Not built from PRD §5's flag list: `--target` (the class decides),
`--conversation` and `--assert` (every invariant is always checked).

**As built [M9] (`conductor bench`, `make bench`).** `make bench` rebuilds
the compose stack from the tree, then `conductor bench` runs load, then each
class `--repeat` times (default 3), waits up to 2 min for every run to end,
and checks every invariant over the run's conversations (prefix
`bench-<unix>-`). It prints PRD §8.2's table and writes
`bench/results/<timestamp>/results.json`, which is not committed. Where each
section comes from:

- LOAD: the clients. Sessions are new conversations sent round-robin to
  the three replicas.
- RECOVERY: the injections above.
- INVARIANTS: `internal/invariant`. I8 shows the guard's decisions and
  `n/a` for violations: nothing after the fact shows a start inside the
  margin, so `TestUnsafeToolNotStartedNearLeaseExpiry` is its falsifier.
- TOOL LEDGER, DUPLICATE EFFECTS, EVENT LOG, max generation: the database.
  A replay is counted per takeover in a chaos conversation: the events
  below the new owner's generation.
- OWNERSHIP counts, I8 decisions, attempt-cap abandons: Prometheus
  `increase()` over the run. Counts a SIGKILLed replica made after its last
  scrape are lost, so they are lower bounds. Their labeled series are
  created at 0 on startup so `increase()` sees a first event.

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

**As built: `conductor inspect`.** `conversation`, `tools` and `runs` read
PostgreSQL directly, as tables or with `--json`. `inspect tool --settle`
overrides only an `ABANDONED` call, to `SUCCEEDED` or `FAILED`, with
`error = 'operator: <reason>'`. Any other state is refused, including a call
already in the requested state, so the operator never gets a silent no-op.
It takes the conversation's lease for the write: it is fenced like any other
write, and refused while a controller owns the conversation. The run stays
failed. `--settle` accepts a unique prefix of the call_id, because `tools`
prints call ids shortened.

**As built: SQLite and `serve` flags.** On SQLite no lease is constructed at
all, rather than one whose `Acquire` returns `ErrUnsupported`. There is no
`--single-process` flag: a single process is the only mode SQLite has.
- `serve` logs a warning on SQLite.
- `serve` refuses to start when `CONDUCTOR_REPLICAS > 1` and the event log
  is SQLite.
- `doctor --config` prints `ownership=postgres-lease` or
  `ownership=process-local`. It also prints one `recovery=` line per
  configured harness (PRD §6.5), from the config alone, without starting
  any harness.
- `serve` takes PRD §5's `--owner-id` and `--lease-ttl`, which override
  `eventlog.postgres.owner_id` and `lease_ttl`, and `--recovery-interval`
  and `--recovery-concurrency`.
