# Conductor — Plan of Record

> Working document. Records the current upstream baseline, the verified gaps,
> the agreed scope, and the implementation order. Update it when a decision
> changes; do not let it drift.

**Conductor** — a vendor-neutral, crash-recoverable fork of Google's AX with
plain-Kubernetes execution, durable tool effects, and first-class operational
telemetry.

Run locally or on standard Kubernetes without requiring Agent Substrate or
Google Cloud services.

---

## 0. Baseline

| | |
|---|---|
| Upstream | `github.com/google/ax` (Apache-2.0) |
| Pinned commit | `b777313` — *Add doctor command to diagnose sidecar, assets, and runtime* (2026-08-19) |
| Policy | Pin and diverge. Do **not** track `main`. Cherry-pick only when a specific upstream change is useful. |
| Upstream PRs | Externally paused during architectural redesign. Upstreaming is upside, never a dependency. |
| Attribution | Apache-2.0 requires retaining notices and stating modifications. README must name the fork and list what Conductor adds. Never present upstream subsystems as originally authored here. |

Upstream size at the pin, non-test Go only:

| Package | LOC |
|---|---|
| `proto` (mostly generated) | 3,362 |
| `internal/harness` | 2,905 |
| `cmd` | 2,195 |
| `internal/skills` | 906 |
| `internal/controller` (incl. `eventlog`) | 740 |
| `internal/pythonsidecar` | 573 |
| `internal/config` | 375 |
| `internal/server` | 206 |
| `internal/ate` | 109 |
| `internal/telemetry` | 106 |
| **Total non-test Go** | **11,644** |

Including tests and Python: 16,311 Go LOC across 68 files, 22 Go test files
(4,667 lines) and 1 Python test file.

The durable-execution core that Conductor targets is small: **~950 LOC**
(`internal/controller` + `internal/server`).

---

## 1. Current architecture (verified against the pinned commit)

```
Client (ax CLI / gRPC)
   │  InteractionsService.CreateInteraction(CreateInteractionEvent)
   │  → stream CreateInteractionResponse
   ▼
internal/server            server.go — gRPC server
   │                       inFlight map[string]struct{} + sync.Mutex   (server.go:43)
   │                       otelgrpc.NewServerHandler()                 (server.go:84)
   ▼
internal/controller        controller.go — Controller.Exec
   │  ├─ logger.ResumptionState()  → (state, harnessID)   (controller.go:224)
   │  ├─ registry.go               → resolve Harness
   │  └─ logger.LogInputs/LogOutputs → eventlog.Append
   ▼
internal/harness           harness.go — Harness / Execution / Handler
   ├─ antigravity/            thin gRPC client → Python sidecar (Gemini)
   ├─ antigravityinteractions/ direct HTTPS/SSE → Vertex Interactions API,
   │                           executes tools in-process
   └─ substrate/              remote gVisor actor via Agent Substrate
                              Start: CreateActor → ResumeActor → dial worker
                                     → waitForHealthy   (substrate.go:87-133)
                              Run:   send turn          (substrate.go:194)
                              Close: SuspendActor       (substrate.go:235-256)
   ▼
internal/controller/eventlog
   sql.go / sqlite.go / postgres.go
   ▼
PostgreSQL or SQLite
```

### Event log schema — the entire persistence layer

Identical DDL in `sqlite.go:44-49` and `postgres.go:45-50`. This is the **only**
`CREATE TABLE` in the repository.

```sql
CREATE TABLE IF NOT EXISTS conversation_log (
    conversation_id TEXT    NOT NULL,
    step            INTEGER NOT NULL,
    payload         TEXT    NOT NULL,   -- protojson-encoded StepEvent
    PRIMARY KEY (conversation_id, step)
);
```

### Protocol surface

`ExecutionService` / `ExecRequest` / `last_seq` were **removed** upstream in
`2282424` (2026-08-11). Current surface:

```protobuf
message CreateInteractionEvent {
  string conversation_id = 1;
  repeated Step inputs   = 2;
  reserved 3;                 // ← last_seq used to live here
  string agent_id        = 4;
  bytes  agent_config    = 5;
}

service InteractionsService {
  rpc CreateInteraction(CreateInteractionEvent)
      returns (stream CreateInteractionResponse);
}

// TODO(jbd): CreateInteraction should return an Interaction message
// and the outputs should be polled from the Interaction.
```

`Step.type` oneof: `ContentStep` | `ThoughtStep` | `ToolCallStep` | `ToolResultStep`.
`enum State`: `UNSPECIFIED | PENDING | FAILED | COMPLETED | CANCELED` — turn-level
only; there is no per-tool-call lifecycle enum, no `WAITING_FOR_APPROVAL`.

### Harness contract — note the documented invariant

`internal/harness/harness.go`, on the `Harness` interface:

> Single-writer expectation: the controller must ensure that at most one
> Execution exists per conversation id at a time. Harness implementations rely
> on this invariant — for example, a harness that durably persists
> per-conversation state may use a last-write-wins store without
> compare-and-swap, which is correct only because there is a single writer per
> conversation.

This matters: see gap G4. Upstream **documents** that harnesses depend on
single-writer, while single-writer is enforced only by an in-process mutex.

---

## 2. Verified gaps

Every claim below was read in the source at the pinned commit.

### G1 — Tool calls and results never reach the event log  *(highest value)*

In `antigravityinteractions`, `Handler.OnMessage` is invoked from exactly one
place — `emitText()`, carrying `res.modelText`. The tool loop:

```go
out := e.harness.executeTool(ctx, call)     // external side effect happens here
next = append(next, toolResultStep{...})    // in-memory slice only, never emitted
```

Tool calls and tool results are never handed to the controller, therefore never
appended to `conversation_log`.

**Precise statement** — the log is not empty, it is *incomplete in one specific
way*: it carries turn-level `State` transitions (`controller.go:175-187` logs
`STATE_COMPLETED` with nil steps), `agent_id` and `agent_config`
(`controller.go:256-263`). What it never carries is tool lifecycle: **the only
`Step` ever appended on this path is the `ContentStep` produced by `emitText`;
`ToolCallStep` and `ToolResultStep` are never appended.**

Consequence: replay-based recovery is not merely unimplemented, it is
*structurally impossible* — you cannot rebuild execution from a log that does
not record what the agent did.

Note the `Handler`/`Step` interfaces already support this (`Step.type` includes
`ToolCallStep` and `ToolResultStep`). The gap is in the harness implementation,
not the protocol. The fix is small and clean.

Not present in the upstream issue tracker.

### G2 — Durable cursor advances before the side effect, and lives on local disk

`antigravityinteractions.Run()` order per turn:

```
postTurn()        → model response + tool calls   (turn committed server-side, Store: true)
prevID = res.interactionID
setPrevID(prevID) → DURABLE WRITE (fsync+rename to a local file)
  ── crash window ──
executeTool(call) → external side effect
postTurn(next)    → results returned to the model
```

Two problems:

1. The cursor advances to the turn *containing* the tool calls before any tool
   runs. Crash in the window and the pending tool calls exist only in the remote
   Interactions service; Conductor never re-fetches them. On resume, `Run()`
   fails with `Run called with no queued input and no work pending`.
2. The only durable local state is a **file** written by `cursorStore`
   (`StateDir`), not the event log or the DB. Another replica cannot read it.

Not present in the upstream issue tracker.

### G3 — Controller does not reconstruct execution state

`internal/controller/controller.go:72`

```go
// TODO(jbd): Resume an incomplete execution if there exists one.
```

The `STATE_PENDING` path calls `h.Start(ctx, conversationID, agentConfig)` then
`exec.Run(...)` with **no intervening `exec.Queue(...)`**.

`ResumptionState()` (`controller.go:224-241`) iterates every event only to pick
the last non-empty `state` and the first non-empty `AgentId`; it discards
`ev.Steps` entirely.

**The user-visible consequence is stronger than the TODO suggests.** Every
harness's `Run` errors on empty input — `antigravity.go:176-178` *"no input
messages queued for execution turn"*, `antigravityinteractions.go:371-376`
*"no input messages queued for the initial turn"* / *"Run called with no queued
input and no work pending"*. Therefore **the `--resume` flag
(`cmd/ax/exec.go:56`, "Resume a conversation without inputs") cannot succeed
today for any harness.** That is a verifiable, user-facing symptom, and it is
exactly what the P0 fix restores.

Upstream: [#190](https://github.com/google/ax/issues/190) (open),
[#229](https://github.com/google/ax/issues/229) (open).

### G4 — Single-writer is process-local only

`internal/server/server.go:43-44`

```go
inFlight   map[string]struct{}
inFlightMu sync.Mutex
```

No lease, fencing token, generation, or ownership column exists anywhere in the
repo. Two server replicas can both believe they own a conversation — which
breaks the invariant the `Harness` interface explicitly documents (§1).

Upstream: [#125](https://github.com/google/ax/issues/125) *Validate the single
writer enforcement* (open).

### G5 — No tool-execution ledger

`ToolCallStep` / `ToolResultStep` carry `id`, `name`, `arguments`, `result`,
`is_error`. There is no per-call lifecycle state, no idempotency key, no attempt
counter, and no representation of an uncertain outcome.

### G6 — Full-history replay only

`eventlog/sql.go:73`

```go
"SELECT payload FROM conversation_log WHERE conversation_id = $1 ORDER BY step"
```

No `LIMIT`, no cursor, no snapshot table, no compaction. Replay cost grows with
history length.

Upstream: [#6](https://github.com/google/ax/issues/6) *Introduce ax snapshot* (open).

### G7 — Observability is trace-only and coarse

**Correction to an earlier assessment:** the OTLP endpoint is *not* hardcoded.
`cmd/ax/serve.go:65-76` reads `cfg.Telemetry.OTLP.Endpoint` and falls back to
`WithInsecure()` for non-GCP endpoints; the config comment names
`localhost:4317` as an example. Traces already work against any OTLP collector.

The real gap:

| | |
|---|---|
| OTel tracing | present — but only **3** hand-written spans repo-wide (`eventlog/sql.go:106`, `substrate.go:195`, `antigravity.go:163`) plus `otelgrpc` auto-instrumentation |
| Configurable OTLP endpoint | present |
| Structured logs | partial (`slog` JSON) |
| **Metrics** | **absent** — zero `Meter()` calls; `otel/metric` is an indirect dependency only |
| **Prometheus / dashboards** | **absent** |
| **Recovery / SLO metrics** | **absent** |

Uninstrumented: `postTurn` (the actual model call), `executeTool`, retries
(`fmt.Fprintf(os.Stderr, ...)` only).

Upstream: [#13](https://github.com/google/ax/issues/13) (open).

### G8 — No *distributed* compute path without Agent Substrate

**Corrected.** Revision 1 of this document made two errors here.

*Error 1 — the dependency claim was wrong.* It said `internal/ate` pulls in
`client-go`, `apimachinery` and `controller-runtime`. It does not.
`internal/ate/client.go` imports only
`github.com/agent-substrate/substrate/pkg/proto/ateapipb` plus gRPC, and the
Substrate dependency is pure protobuf/gRPC:

```
$ grep -c 'k8s.io' go.sum                      → 0
$ grep -c 'controller-runtime' go.sum           → 0
$ go list -deps ./internal/ate | grep -c k8s.io → 0
```

This **reverses the cost estimate for block (3)**: a plain-Kubernetes executor
introduces the module's *first* Kubernetes client dependency — `client-go` +
`apimachinery` + `api` and roughly fifteen transitive modules, plus informer
lifecycle, RBAC, ServiceAccounts, in-cluster vs kubeconfig auth, pod-to-pod
addressing, harness image publishing, and a `kind` CI job. It is the largest
single block, not an afternoon of Pod lifecycle.

*Error 2 — "no compute path without Agent Substrate" overstated it.* A local
path does exist: `internal/harness/antigravity` runs a **local subprocess**
sidecar (`antigravity.go:71-105` via `pythonsidecar.Sidecar`), and `ax exec`
without `--server` runs an in-process controller (`cmd/ax/exec.go:63`). What is
missing is a **non-Substrate distributed** path.

There is still no `Executor` abstraction and no plain-Pod backend
(`grep -ri "docker\|isolation"` over `internal/` and `cmd/`: zero hits).

`manifests/README.md:5`:

> This path is experimental and incomplete: the manifests, scripts, and …

### G9 — Model path: architecture is agnostic, built-ins are not

AX's architecture *is* harness/model agnostic — `HarnessService` is the
documented extension boundary. What is missing is a usable **non-Google
reference harness**. All three built-ins require Gemini/Vertex/Antigravity or
Agent Substrate. There is no OpenAI-compatible path.

State the gap as "no non-Google reference harness", **not** "the runtime is
bound to Gemini". The latter is inaccurate and easy to refute.

### G10 — HITL exists in the proto, not in the runtime

`ConfirmationContent` / `ApprovalDecision` / `DeclineDecision` exist in
`proto/content.proto:28-45`, but there is no `WAITING_FOR_APPROVAL` state in the
`State` enum and the only consumer is the interactive CLI loop in
`cmd/ax/exec.go:202-284`, which blocks in-process. If the CLI dies the pending
approval is lost. Out of scope for Conductor.

### G11 — `StepEvent.interaction_id` is a published dead field

```
proto/ax.proto:32          string interaction_id = 2;        declared
controller.go:207          interactionID string              declared
controller.go:258, :270    InteractionId: l.interactionID     read
entire repository          assigned nowhere
```

Meanwhile `antigravityinteractions` holds the remote cursor in
`res.interactionID` and persists it to a **local file** (`cursorstore.go`).

The field whose published purpose is exactly to carry a remote interaction
cursor is unused. Threading `res.interactionID` into the log is the cheapest fix
for G2 and would make I6 true for that harness — **additive, no protocol
change**. Blocked only by `Handler.OnMessage` having no slot for it while the
harness interfaces are frozen; recorded in `correctness.md` §1 (I6) as a known
path, not scheduled.

### Dead ends — do not plan work here

| Item | Why |
|---|---|
| `last_seq` replay (#320) | Closed 2026-08-13: *"We are closing this issue and will come back to it after the protocol revamp."* The field is gone; `CreateInteractionEvent` has `reserved 3`. |
| Postgres dashboard parity (#307) | The dashboard was deleted upstream in `c0d8e5b` (2026-07-22). The gap no longer exists. |
| DurableDir (#268) | Lives in `agent-substrate/substrate`, a separate repository. Not actionable in an AX fork. |

---

## 3. Scope

Six blocks. Each has a Definition of Done. **If it cannot be written into a DoD,
it is not in scope.**

### ① Bootstrappable

Two harnesses and a one-command local stack.

1. **`fake`** — deterministic, implements `harness.Harness` / `harness.Execution`
   with programmable actions (think, tool call, delay, fail-once,
   crash-after-effect, duplicate callback) and an append-only `fake_effects`
   table so I2 is a SQL assertion.
2. **`openaicompat`** — `base_url` + `model`, covering OpenAI, Ollama, vLLM,
   llama.cpp, Together, Fireworks, Groq, OpenRouter, DeepSeek and Gemini's
   compatibility endpoint. **Promoted from P1** (see below).
3. **`internal/harness/remote`** — generic `HarnessService` gRPC client, ~80 LOC,
   extracted from the logic duplicated in `substrate.go:194-233` and
   `antigravity.go:163-200`. Every executor backend needs it; extract it before
   block ③ rather than during.
4. `docker compose` stack, with an optional Ollama profile.

**DoD** — `git clone && docker compose up` brings up PostgreSQL + Conductor +
`fake` + Prometheus + Grafana; a full execution runs end to end with **zero API
keys**; pointing `OPENAI_BASE_URL` at a local model runs the same agent; the
same path runs in CI.

*Why `fake` is first: block ⑥ is impossible without it — real model calls are
nondeterministic, rate-limited, and cost money. Every benchmark number
originates there.*

*Why `openaicompat` is no longer P1:* only a harness where **Conductor owns the
tool loop** can be recovered at all — upstream's `HarnessRequest` oneof is
`{start, cancel}` and `antigravityinteractions/server.go:143-151` states that
carrying input after `start` is intentionally unsupported, so a reconciled tool
result can never reach a harness-internal model loop. `fake` and `openaicompat`
are the only `recovery: full` harnesses. Without `openaicompat`, every recovery
number is measured against a harness this project wrote itself. Budget 3–5 days,
not one — tool-calling behaviour varies across providers.

### ② Complete effect log + recovery

- Emit `ToolCallStep` / `ToolResultStep` through `Handler.OnMessage` so tool
  lifecycle reaches the event log (G1).
- Durable tool ledger: stable `call_id`, argument-hash idempotency key, attempt
  counter, terminal states including `OUTCOME_UNKNOWN` (G5).
- Rebuild execution state from the log and `Queue()` it before `Run()` on
  `STATE_PENDING` (G3).
- Close the cursor/effect ordering window (G2).

Effect semantics under partial failure:

```
TOOL_PLANNED  → stable call_id + idempotency_key persisted
TOOL_STARTED  → persisted
              → external side effect
TOOL_SUCCEEDED / TOOL_FAILED

crash between TOOL_STARTED and a terminal state
              → OUTCOME_UNKNOWN

  idempotent / read-only      → retry with the same idempotency key
  reconcilable                → query the external system, then resolve
  non-idempotent, unqueryable → recovery policy / manual resolution
```

**Never claim exactly-once external side effects.** The claim is
*crash-safe, failure-aware tool execution with durable idempotency and
uncertain-outcome reconciliation*.

**DoD** — after any injected crash, the event log alone can reconstruct which
tools were called, with what arguments, what each returned, how many attempts
occurred, and the terminal state of each; the `OUTCOME_UNKNOWN` path has tests.

### ③ Plain-Kubernetes executor

**Narrowed.** This is the largest block, not the smallest: it introduces the
module's *first* Kubernetes client dependency (see the corrected G8 — `go.sum`
contains zero `k8s.io/` entries today), plus informer lifecycle, RBAC,
ServiceAccounts, in-cluster vs kubeconfig auth, pod-to-pod addressing, harness
image publishing, and a `kind` CI job.

```
Executor
├── local          (subprocess)        ← enough for blocks ② ④ ⑥
└── KubernetesPod  (ours, narrowest)
```

`local` comes first and carries blocks ② and ④ and **all of block ⑥'s
numbers** — chaos kills the *controller* and the *harness process*, and a
subprocess can be killed exactly as hard as a pod.

`KubernetesPod` does only what its DoD requires: `client-go` typed
`Create`/`Watch`/`Delete`, a single namespace, one static manifest set in
`deploy/k8s/`, one `kind` script.

**Cut from this block:** PodDisruptionBudget, resource tuning, gVisor, and the
rolling-update assertion — **that assertion moves to block ④**, where it is
about controller replicas and leases rather than pods. Asserting it in both
places pays twice for one signal.

**Cut entirely: the Agent Substrate executor adapter.** It cannot be tested
without a Substrate-equipped cluster, and "kept for compatibility" is a
maintenance promise this project cannot honour. The `substrate` harness package
stays in the tree, unwired, documented as unsupported. Apache-2.0 attribution is
unaffected.

**Scope fence:** Pod lifecycle only. **No CRD, no operator, no custom scheduler,
no autoscaler.**

**DoD** — on `kind`, with Agent Substrate absent, deleting the executor Pod lets
the session recover and complete. One assertion.

### ④ Distributed ownership

PostgreSQL lease + fencing generation (or K8s `Lease`); every append is a
conditional write gated on the current generation. Multiple controller replicas,
single-writer invariant preserved across failover.

**No Raft. No custom consensus.**

**DoD** — three controller replicas; killing the holder transfers ownership; a
write from the stale holder carrying an expired generation is rejected by the
persistence layer.

### ⑤ Operability

Structured logs carrying `conversation_id`, `trace_id`, owner identity, and step
index. Metrics: active sessions, executions total, execution duration, recovery
count / failures / duration, replay count and size, tool attempts, duplicate
effects, controller errors, executor readiness latency. End-to-end spans:
client → server → controller → executor → model → tool → event append →
recovery. Grafana dashboard checked into the repo.

**Sequencing note:** the *logs + a small set of metrics* must land **before**
blocks ③ and ④ — pod-level failure recovery is not debuggable without them. Full
tracing and dashboards can follow.

**DoD** — one trace spans the full path; a Grafana panel answers "how long did
the last recovery take, and how many tool attempts did it involve".

### ⑥ Chaos + load benchmark

Injected failures: client disconnect, controller process/pod kill, executor pod
kill, duplicate request, slow downstream, model/tool timeout, transient DB
failure, rolling restart.

Measured: max concurrent sessions, throughput, p50/p95/p99 latency, recovery
success rate, p95 recovery latency, event loss, duplicate effects, error rate.

**DoD** — 8 failure classes × N injections; every invariant in §4 asserted, not
merely "did it recover"; reproducible with one command.

---

## 4. Correctness invariants

**Superseded by `ARCHITECTURE.md` §4**, which is the authoritative set (I1–I8).
It tightens I1 (a run may not reach `COMPLETED` while any tool call is
`STARTED` or `OUTCOME_UNKNOWN`) and adds I7 (projections are reconstructible
from the log) and I8 (do not start an `UNSAFE` call you cannot commit before
the lease expires).

Block (6) asserts each invariant individually, not merely "did it recover".

## 5. Anti-scope-creep rules

1. **Tool ledger: three categories, three policies. No plugin system.**
   Idempotent → retry with the same key. Reconcilable → query the external
   system. Neither → `OUTCOME_UNKNOWN` + policy. Three `switch` arms. No policy
   DSL, no effect-plugin framework, no generic saga engine.
2. **`KubernetesPodExecutor` does Pod lifecycle, not scheduling.** No CRD, no
   operator, no custom scheduler, no autoscaler.
3. **Anything without a DoD does not get built.** This prevents mid-flight
   feature growth rather than cutting half-written work later.

---

## 6. Implementation order (locked)

Milestone estimates and the full breakdown live in `PRD.md` §7.

```
 0  Pin b777313, fork, read the streaming path end to end
    (internal/controller 740 LOC, internal/server 206 LOC)
 1  ①  internal/harness/remote (generic gRPC client, ~80 LOC)
 2  ①  fake harness + fake_effects + docker compose, one command, zero API keys
 3      docs/conductor/correctness.md — I1..I10, each with its falsifying query
 4  ②  Tool call/result into the event log; stable self-generated call_id,
        idempotency key, attempts, OUTCOME_UNKNOWN, reconcile_deadline
 5  ②  I9 + I10 first: stop swallowing append errors (controller.go:153-162),
        add event_id so the resulting retry is safe.  Recovery depends on this.
 6  ②  Reconciler: rebuild from the log, Queue() before Run() on PENDING,
        poison guard, terminal-state short circuit
 7  ①  openaicompat harness — the only shippable recovery:full harness
 8  ⑤a Structured logs + core metrics (enough to debug; dashboards later)
 9  ④  PostgreSQL lease + fencing + multi-replica failover + rolling update
10  ③  local executor, then the narrowest KubernetesPod on kind
11  ⑤b Full-path tracing + Grafana dashboard
12  ⑥  Chaos + load benchmark → real numbers
── P1 ────────────────────────────────────────────────────────
    Resumable client stream protocol (clean redesign, not a #320 revival)
    Anthropic adapter · gVisor RuntimeClass · event-log snapshot/compaction (G6)
```

Three ordering changes from revision 1, all forced by the design review:

- **`openaicompat` moved out of P1** into block ①, because only a harness whose
  tool loop Conductor owns can be recovered (§3 block ①). Without it every
  recovery number comes from a harness this project wrote itself.
- **I9/I10 moved ahead of the reconciler.** Recovery replays the log, so the log
  must first be complete *and* the append path must stop delivering steps it
  failed to persist. Fixing recovery on top of a lossy append is building on
  sand.
- **Ownership (④) now precedes the Kubernetes executor (③).** Ownership is
  ~200 LOC against a database and carries the highest-density resume claim;
  the Kubernetes executor is the largest block and the most likely place to
  stall. If the schedule gives, it should give there — not in ②, ④ or ⑥, which
  is where the correctness signal lives.

---

## 7. Explicit non-goals

Public `pkg/` API extraction · a provider matrix inside the runtime · Anthropic
adapter (P1) · resumable client stream protocol (P1) · gVisor (P1) · **Agent
Substrate executor adapter** · Raft or custom consensus · Firecracker · Kafka ·
service mesh · multi-cloud · agent memory · RAG · model training or SFT · voice ·
multi-agent planning · evaluation framework · Kubernetes CRD or operator ·
durable HITL · frontend work.

Rationale for the three most likely to be reopened:

- **`internal/` → `pkg/` extraction.** For a runtime, the extension boundary is
  the gRPC `HarnessService`, not Go imports. `internal/` signals "no public API
  commitment", which is reasonable for a project openly warning about breaking
  changes. The refactor costs package cycles, naming, config ownership, and test
  churn while producing almost no distributed-systems signal.
- **A provider matrix.** One OpenAI-compatible harness covers OpenAI, Ollama,
  vLLM, llama.cpp and most hosted inference endpoints through a single
  interface. `OpenAIProvider` + `AnthropicProvider` + `GeminiProvider` inside
  the runtime turns it into a model gateway — four API clients, four streaming
  formats, four tool-call schemas — which is a different project and dilutes
  the durable-execution narrative. Model neutrality is architectural (the
  harness boundary), not a feature list.
- **Agent Substrate executor adapter.** Untestable without a Substrate-equipped
  cluster, and "kept for compatibility" is a maintenance promise this project
  cannot honour. The harness package stays in the tree, unwired and documented
  as unsupported.

Also explicitly *not guaranteed* (see `correctness.md` §0 and §3): exactly-once
external side effects; at most one *running* process per conversation (only one
*committing* writer); recovery for harnesses that own their own tool loop;
multi-replica correctness on SQLite; bounded replay cost before snapshots land.

---

## 8. Positioning

**Tagline**

> Conductor — a vendor-neutral, crash-recoverable fork of Google's AX with
> plain-Kubernetes execution, durable tool effects, and first-class operational
> telemetry.
>
> Run locally or on standard Kubernetes without requiring Agent Substrate or
> Google Cloud services.

Do **not** use "AX, without Google Cloud" — AX is architecturally
harness/model-agnostic and a maintainer can refute the claim in one line.

**Selling points, in order of what a reader perceives first**

1. Runs anywhere. `docker compose up`, zero API keys, any OTLP collector, plain
   Kubernetes.
2. The execution log is actually complete — tool calls and results are recorded,
   so recovery can be reconstructed from the log.
3. Effect semantics under partial failure — durable idempotency keys and
   explicit uncertain-outcome reconciliation.
4. Proven, not claimed — a reproducible chaos and load benchmark ships with the
   repo.

**README attribution block (required)**

> Conductor is a fork of [google/ax](https://github.com/google/ax) (Apache-2.0).
> Upstream provides the distributed harness runtime, the gRPC protocol, the
> event log, and the Agent Substrate integration. Conductor adds: …

---

## 9. Resume bullets (numbers from the `make bench` run in PRD §8.3)

Structure: correctness/durability → distributed ownership + Kubernetes →
observability + benchmark. Vocabulary drawn from real 2026 postings (Amazon
AgentCore: *"agent execution, session state, tool orchestration … reliability,
and low-latency streaming"*; Anthropic: *"failure, retries, idempotency, and
consistency"*; Moveworks: *"tracing, logging, and metrics frameworks … latency
bottlenecks, race conditions, throughput limitations"*).

**Conductor — Vendor-Neutral Agent Execution Runtime** *(Go, gRPC, PostgreSQL,
Kubernetes, OpenTelemetry)*

- Extended Google's open-source agent runtime into a vendor-neutral distributed
  execution runtime in Go, making the execution log complete and replayable:
  persisted tool-call lifecycle with stable IDs, argument-hash idempotency keys,
  and per-attempt state, then implemented event-sourced recovery so restarted
  controllers resume long-running agent sessions without losing or duplicating
  tool effects — **24 of 24 injected failures recovered across
  6 failure classes, 0 duplicate effects over 27,792 idempotent tool calls**.
- Implemented distributed session ownership with PostgreSQL leases and fencing
  tokens, preserving single-writer consistency across M controller replicas
  through pod deletion, failover, and rolling updates; replaced the
  Substrate-only compute path with a plain-Kubernetes executor owning Pod
  lifecycle, readiness, and graceful drain — **in the compose benchmark,
  24/24 sessions finished after controller, harness and database failures
  across 3 replicas, p95 10.3 s from failure to finished session**.
- Made the runtime operable: extended trace-only telemetry into a full
  observability stack with Prometheus metrics and end-to-end distributed tracing
  across client → controller → executor → model → tool → event log, and shipped
  a reproducible chaos/load benchmark reporting **4,628 sessions in 60 s at
  8 concurrent, run latency p99 149 ms, 0 failures**.

Never write "exactly-once tool execution".

Portfolio split — the two projects must not overlap:

| ProxyLoop | Conductor |
|---|---|
| Model + Agent | Runtime + Systems |
| Qwen3-8B SFT/QLoRA, teacher-generated trajectories, held-out and false-completion eval, Fast/Slow agent, voice, Temporal workflows | Go, distributed systems, Kubernetes, reliability, concurrency, observability, production operations |
| 58% → 67% completion, 6% → 2% false completion | recovery rate, duplicate effects, p95/p99, throughput |

Do not add Go/Kubernetes to ProxyLoop, and do not add model/agent features to
Conductor.

---

## 10. Open risks to retire in week one

1. **Does Agent Substrate run on `kind`?** If not, block ③ deploys the
   controller plus a plain-Pod harness and skips Substrate entirely. Either
   answer is fine — discovering it in week five is not.
2. **Upstream protocol revamp.** `CreateInteraction` is mid-redesign (see the
   `TODO(jbd)` in `proto/ax.proto`, and #320's deferral). The pin insulates us;
   do not resync opportunistically.
3. **Benchmark honesty.** Every number in §9 must come from a reproducible run
   of block ⑥. No estimates, no extrapolation.
