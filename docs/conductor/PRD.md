# Conductor — Product Requirements

> What the finished product is, from the point of view of someone deciding
> whether to adopt it. Companion to `PLAN.md` (scope and ordering) and
> `ARCHITECTURE.md` (design and invariants). Where this document and those
> disagree about scope, `PLAN.md` wins.

Module: `github.com/simondxu/conductor` · Fork of
[`google/ax`](https://github.com/google/ax) (Apache-2.0) pinned at `b777313`.

Every number in this document is a placeholder of the form `<...>`. No
benchmark result in here has been measured. Values are filled in only by a
reproducible run of `make bench` (§8).

---

## 1. Who this is for

Three readers, in order of how much the product is shaped around them.

**1. The engineer who already has an agent loop and now has an operations
problem.** They have something that calls a model, gets tool calls back, runs
the tools, and loops. It works on a laptop. Then it has to run as a service:
multiple replicas, a database, Kubernetes, a pager. The questions they cannot
currently answer are: if this process dies halfway through a tool call, what
happened to the tool call? If I run two replicas, what stops both of them from
driving the same conversation? How long was the last session stalled, and why?

**2. The engineer who wants a durable agent runtime but not a vendor.** They
want the execution, persistence, and recovery machinery without adopting a
specific model provider, a specific cloud, or a managed control plane. They may
be running a local model for cost or data reasons.

**3. The reader evaluating the fork.** Someone who knows `google/ax` and wants
to know precisely what Conductor changed, what it claims, and whether the
claims are tested.

### Problem statement in plain terms

Upstream AX persists a conversation log, but the log records only the model's
text. Tool calls and tool results are never written to it
(`internal/harness/antigravityinteractions/antigravityinteractions.go:396-435`
executes each tool and appends the result to an in-memory slice;
`Handler.OnMessage` is reached only from `emitText`). So the log cannot tell you
what the agent did — only what it said.

That has three consequences:

- **Recovery is not possible, not merely unimplemented.** `internal/controller/controller.go:72`
  carries `// TODO(jbd): Resume an incomplete execution if there exists one.`
  You cannot rebuild an execution from a log that does not record its effects.
- **Single-writer ownership is process-local.** `internal/server/server.go:43-44`
  guards concurrent work with an in-process `map` plus a `sync.Mutex`. Two
  replicas can both believe they own a conversation. The `Harness` interface
  doc comment (`internal/harness/harness.go:34-41`) explicitly states that
  harness implementations *rely* on single-writer being true.
- **There are no metrics.** Zero `Meter()` calls in the repository; three
  hand-written trace spans in total. The model call and the tool call — the two
  things that actually take time and fail — are uninstrumented.

Conductor's job is to close those three gaps, in that order, and to prove it
with a benchmark anyone can re-run.

### What Conductor claims, and what it does not

Claimed: **crash-safe, failure-aware tool execution with durable idempotency
and uncertain-outcome reconciliation.**

**Conductor does not claim exactly-once external side effects, and never will.**
No system that calls someone else's API over a network can promise that. What it
promises is that after any crash you can determine, from durable state alone,
which tools were called, with what arguments, how many times, and whether the
outcome is known. Where the outcome is genuinely unknown, Conductor says so
explicitly rather than guessing.

A second, narrower limit, stated up front because it is load-bearing:
**execution recovery works for harnesses whose tool-execution loop Conductor
owns.** Conductor supplies two such harnesses (`fake` and `openai-compat`). The
harnesses inherited from upstream (`antigravity`, `antigravity-interactions`,
`substrate`) run their tool loops internally and keep conversation state in a
remote service, so Conductor cannot reconstruct or resume them; they are marked
non-recoverable and the recovery loop skips them. See §6.5.

---

## 2. Adoption path

The whole path below uses no API keys and no cloud account.

### Step 0 — prerequisites

Docker (with Compose v2), `make`, and `git`. Go 1.26+ only if you want to build
outside Docker.

### Step 1 — clone and start the stack

```
git clone https://github.com/simondxu/conductor
cd conductor
docker compose -f deploy/compose/docker-compose.yml up
```

This starts five containers:

| Service | Purpose | Port |
|---|---|---|
| `postgres` | event log, tool ledger, leases | 5432 |
| `conductor` | controller + gRPC `InteractionsService` | 8494 |
| `fake-harness` | deterministic harness, serves `HarnessService` | 50053 |
| `prometheus` | scrapes `conductor:9090/metrics` | 9090 |
| `grafana` | the checked-in dashboard, provisioned | 3000 |

What you see: the `conductor` container runs `conductor migrate up` to the
current schema version, then logs one line per applied migration and one
`server listening` line in JSON. Grafana at `http://localhost:3000` shows the
`Conductor / Runtime` dashboard with every panel at zero.

### Step 2 — run an agent against the deterministic harness

```
docker compose exec conductor \
  conductor run --agent fake --input "book me a flight"
```

The `fake` harness runs a scripted plan: think, call one `IDEMPOTENT` tool, call
one `UNSAFE` tool, answer. You see streamed steps on stdout and a conversation
id on the last line. The Grafana `Runs` panel moves from 0 to 1 and back.

### Step 3 — point it at your own model

Edit `conductor.yaml` (mounted into the container) to register the
OpenAI-compatible harness, then restart `conductor`:

```yaml
registry:
  openai-compat:
    default: true
    base_url: http://host.docker.internal:11434/v1   # Ollama
    model: qwen3:8b
    api_key_env: OPENAI_API_KEY                      # unset is fine for Ollama
    request_timeout: 120s
    max_turns: 24
    tools:
      - name: http_get
        class: IDEMPOTENT
      - name: send_email
        class: UNSAFE
```

```
docker compose restart conductor
docker compose exec conductor conductor doctor
```

`doctor` prints one line per check. For this configuration it reports the
backend, the schema version, whether `base_url` answers, whether the model
reports tool-calling support, and whether the harness is recoverable:

```
backend          postgres 17.x        ok
schema           version <N>          ok
harness          openai-compat        ok
  base_url       http://.../v1        reachable (<M> ms)
  model          qwen3:8b             tool_calls: supported
  recovery       owned tool loop      recoverable
telemetry        otlp disabled        ok
metrics          :9090/metrics        ok
ownership        lease (postgres)     ok
```

Then run the same command as Step 2 with `--agent openai-compat`.

### Step 4 — survive a crash

```
# terminal 1: start a run that will be interrupted mid-tool-call
docker compose exec conductor \
  conductor run --agent fake --conversation demo-1 \
                --input "charge the card then email the receipt" &

# terminal 2: kill the controller after the UNSAFE tool has started
docker compose kill -s KILL conductor
docker compose start conductor
```

What you see, in order:

1. The client in terminal 1 gets a gRPC `UNAVAILABLE` and exits non-zero. The
   run is not lost — it is unowned.
2. The restarted controller's recovery loop finds `demo-1` within one poll
   interval (default `<5s>`), acquires the lease at generation 2, and logs
   `recovery.start` / `recovery.complete` with the conversation id and the
   number of unsettled tool calls.
3. `conductor inspect conversation demo-1` shows the full history including the
   tool call that was in flight, now in state `OUTCOME_UNKNOWN` and then
   resolved per its class.
4. The Grafana `Recovery` panel shows one recovery with its duration.

To re-attach a client to the recovered conversation:

```
docker compose exec conductor conductor run --conversation demo-1 --resume
```

**In P0 this attaches to the live stream and shows steps emitted from that point
on. It does not replay steps you missed while disconnected.** Use
`conductor inspect conversation demo-1` to read the complete history. Catch-up
replay on reconnect is P1; see §6.

---

## 3. Bring your own model

### 3.1 Why there is one harness and not a provider matrix

Conductor contains no provider SDKs. The extension boundary is the gRPC
`HarnessService` (`proto/ax.proto:80-87`), and the `openai-compat` harness takes
two values — `base_url` and `model` — and speaks the OpenAI chat-completions
wire format over them. That one harness reaches local runtimes (Ollama, vLLM,
llama.cpp, LM Studio) and hosted providers that expose an OpenAI-compatible
endpoint, without Conductor knowing anything about them.

Building a provider matrix would turn the runtime into a model gateway, which is
a different product. Anthropic has no OpenAI-compatible endpoint and gets a
native adapter in P1; until then, point `base_url` at a proxy.

### 3.2 Config surface

Complete set of knobs. Anything not listed does not exist.

| Key | Required | Meaning |
|---|---|---|
| `base_url` | yes | OpenAI-compatible base, including `/v1` |
| `model` | yes | Model id as the endpoint names it |
| `api_key_env` | no | **Name of an environment variable** holding the key. Conductor never takes a key inline in config and never logs it. Unset means no `Authorization` header. |
| `request_timeout` | no | Per-request timeout. Default `<120s>`. |
| `max_turns` | no | Hard cap on model↔tool round trips per run. Default `<24>`. |
| `tool_choice` | no | Passed through: `auto` \| `none` \| `required`. Default `auto`. |
| `extra_headers` | no | Map of header name to env-var name, for gateways that need routing headers. |
| `tools[].name` | yes per tool | Tool name sent in the request. |
| `tools[].class` | yes per tool | `IDEMPOTENT` \| `RECONCILABLE` \| `UNSAFE`. Declared, never inferred. Determines recovery policy (§6.3). |

There is no fallback chain, no routing, no retry-across-providers, and no cost
model. One harness instance points at one endpoint.

### 3.3 Honest limits

- **Chat completion is portable. Tool calling is not.** Endpoints differ on
  whether `tools` is accepted, whether `tool_choice` is honoured, whether
  arguments arrive as valid JSON, and whether `tool_call_id` is stable across
  retries. Small local models frequently emit malformed tool arguments. The
  supported claim is "any OpenAI-compatible endpoint **with working tool
  calling**", and §3.4 records what was actually exercised.
- **Conductor does not use the provider's tool-call id for identity.** Call
  identity is `call_id = hex(sha256(conversation_id || step || tool_name ||
  args_hash))[:32]`, computed by Conductor from durable inputs. Provider ids are
  recorded for correlation only. This is why a provider that changes its ids
  between retries does not break idempotency.
- **Token and cost metrics are absent for most local models.**
  `conductor_model_tokens_total` is emitted only when the endpoint returns a
  `usage` block. Local runtimes often do not. The metric is simply not recorded
  in that case — it does not report zero, and dashboards show "no data" rather
  than a misleading flat line. There is no cost metric at all; Conductor does
  not know prices.
- **No streaming-token logging.** Conductor appends one log event per completed
  step, not per token delta. Partial token streams reach the client but are not
  individually durable. This is deliberate: one DB transaction per token is not
  a design, it is an outage.
- **No prompt management, no evals, no fine-tuning, no retrieval.** See §10.

### 3.4 Tested combinations (table structure for the README)

The README carries this table. A row exists only after the listed check has been
run against that combination; there are no inferred rows.

| Endpoint | `base_url` form | Model | Tool calling | Parallel tool calls | `usage` returned | Chaos suite | Notes |
|---|---|---|---|---|---|---|---|
| Ollama | `http://host:11434/v1` | `<model>` | `<pass/fail>` | `<yes/no>` | `<yes/no>` | `<pass/fail/not run>` | `<...>` |
| vLLM | `http://host:8000/v1` | `<model>` | `<pass/fail>` | `<yes/no>` | `<yes/no>` | `<pass/fail/not run>` | `<...>` |
| llama.cpp server | `http://host:8080/v1` | `<model>` | `<pass/fail>` | `<yes/no>` | `<yes/no>` | `<pass/fail/not run>` | `<...>` |
| OpenAI | `https://api.openai.com/v1` | `<model>` | `<pass/fail>` | `<yes/no>` | `<yes/no>` | `<pass/fail/not run>` | `<...>` |
| `<other>` | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` |

Column meanings:

- **Tool calling** — the conformance run (`conductor doctor --harness <id>
  --probe-tools`) got a well-formed tool call and accepted a tool result.
- **Parallel tool calls** — the endpoint returned more than one tool call in a
  single assistant message and Conductor executed them.
- **`usage` returned** — whether `conductor_model_tokens_total` is populated.
- **Chaos suite** — whether `make bench` ran its failure classes against this
  endpoint rather than against `fake`. Expect most rows to say "not run": the
  benchmark's reproducible numbers come from `fake` by design (§8.1).

---

## 4. Monitoring

### 4.1 What an operator sees

Three surfaces:

- **Structured logs** (JSON on stdout). Every line carries `conversation_id`,
  `trace_id`, `owner_id`, `generation`, and `step` where applicable. The
  recovery path logs `recovery.start`, `recovery.rebuild`, `recovery.settle`,
  `recovery.complete` with counts.
- **Prometheus metrics** on `:9090/metrics`. The full set is defined in
  `ARCHITECTURE.md` §7.2 and is not restated here.
- **A Grafana dashboard** checked into `deploy/compose/grafana/`, provisioned
  automatically by `docker compose up`. No manual import.

### 4.2 Dashboard panels

Each panel exists to answer one question. A panel that answers nothing gets
deleted.

| Panel | Question it answers | Metrics |
|---|---|---|
| Active runs by owner | How is work distributed across replicas, and did a replica stop taking work? | `conductor_runs_active` |
| Run outcomes | What fraction of runs complete vs fail vs cancel? | `conductor_runs_total` |
| Run duration | Is end-to-end latency drifting? | `conductor_run_duration_seconds` |
| Recovery | Did anything recover recently, did it succeed, and how long was the session stalled? | `conductor_recovery_total`, `conductor_recovery_duration_seconds` |
| Replay cost | Is replay getting more expensive as histories grow? (the panel that justifies snapshots) | `conductor_replay_events_total` |
| Lease churn | Is ownership stable, or are we flapping? | `conductor_lease_acquisitions_total`, `conductor_lease_losses_total{reason}` |
| Fenced writes | Did a stale owner try to write, and was it rejected? Direct evidence for I4. | `conductor_fenced_writes_rejected_total` |
| Tool ledger state | How many calls are unsettled right now, and in which state? | `conductor_tool_calls_total{state}` |
| Tool retry pressure | Are we re-running tools, and which ones? | `conductor_tool_attempts_total` |
| Unknown outcomes | How often is a tool outcome genuinely unknown, and how is it resolved? | `conductor_tool_outcome_unknown_total{resolution}` |
| Duplicate effects | Did an idempotent tool produce a second effect? Direct evidence for I2. | `conductor_duplicate_effects_total` |
| Model latency | Is the model or the runtime the bottleneck? | `conductor_model_duration_seconds` |
| Executor cold start | What does acquiring an execution environment cost per backend? | `conductor_executor_acquire_duration_seconds` |

The two panels an operator looks at first during an incident are **Recovery**
and **Tool ledger state**: together they answer "is anything stuck, and is
anything mid-effect".

### 4.3 Alerting

These are starting thresholds to tune against your own traffic, not measured
values. Every one is a guess until you have a baseline.

| Alert | Condition | Severity | Why |
|---|---|---|---|
| Unsettled tool calls | `conductor_tool_calls_total{state=~"STARTED\|OUTCOME_UNKNOWN"}` > 0 for > `<10m>` | page | A real-world effect is in an unknown state and nothing is resolving it. This is the one that justifies waking someone. |
| Duplicate effect | `increase(conductor_duplicate_effects_total[5m])` > 0 | page | I2 violated. Should be impossible; treat as a correctness bug, not capacity. |
| Fenced writes sustained | `rate(conductor_fenced_writes_rejected_total[5m])` > 0 for > `<5m>` | ticket | Fencing is working, but something keeps trying — usually a replica that is alive but partitioned from the DB. |
| Recovery failing | `increase(conductor_recovery_total{result="failure"}[15m])` > `<3>` | page | Recovery itself is broken; sessions will accumulate. |
| Recovery stall | `histogram_quantile(0.95, conductor_recovery_duration_seconds)` > `<2 × lease TTL>` | ticket | Recovery is running but slow; usually replay cost or executor cold start. |
| Lease flapping | `rate(conductor_lease_losses_total[10m])` > `<1/min>` | ticket | GC pauses or DB latency are causing false preemption. Tune TTL. |
| No active work | `sum(conductor_runs_active)` == 0 for > `<15m>` during expected traffic | ticket | Catches a silently wedged controller that is still reporting healthy. |
| Run failure rate | `rate(conductor_runs_total{outcome="failed"}[10m]) / rate(conductor_runs_total[10m])` > `<0.05>` | ticket | Generic health. |

Deliberately absent: an alert on model latency. Model latency is a property of
whatever endpoint you pointed at, and Conductor has no opinion about it.

---

## 5. CLI surface

Six commands. `conductor exec` remains as a hidden alias for `conductor run` for
one release.

### `conductor serve`

```
conductor serve [--config conductor.yaml]
                [--owner-id <string>]        # default: hostname + pid
                [--metrics-addr :9090]
                [--lease-ttl 30s]
                [--recovery-interval 5s]
                [--recovery-concurrency 4]
```

Runs the controller: gRPC `InteractionsService`, the recovery loop, the metrics
endpoint, and gRPC health. Logs JSON to stdout. On `SIGTERM` it stops accepting
new work, releases its leases, drains in-flight turns, and exits — so planned
restarts hand over immediately instead of waiting out a lease TTL.

### `conductor run`

```
conductor run [--conversation <id>]       # generated if absent
              [--agent <harness-id>]      # default harness if absent
              [--input <text> | --resume]
              [--agent-config <json> | --agent-config-file <path>]
              [--server host:port]        # omit to run an in-process controller
              [--config conductor.yaml]
```

`--input` and `--resume` are mutually exclusive, as are `--agent-config` and
`--agent-config-file`. Example:

```
$ conductor run --agent fake --input "charge the card then email the receipt"
conversation: 7b3c1f2a-...
[thought]      planning two steps
[tool_call]    charge_card{amount:4200}  call_id=9f2a... class=IDEMPOTENT
[tool_result]  charge_card -> {"status":"ok","ref":"ch_<...>"}
[tool_call]    send_email{to:"a@b.c"}    call_id=4d81... class=UNSAFE
[tool_result]  send_email -> {"status":"sent"}
[content]      Charged and emailed. Reference ch_<...>.
completed      steps=<N>  duration=<...>s
```

### `conductor inspect`

Read-only. Three subcommands.

```
conductor inspect conversation <id> [--from-step N] [--json]
conconductor inspect tools <conversation-id> [--unsettled-only] [--json]
conductor inspect runs [--state PENDING|COMPLETED|FAILED|CANCELED] [--limit N]
```

```
$ conductor inspect tools 7b3c1f2a --unsettled-only
call_id   tool         class    state            attempts  started_at  gen
4d81...   send_email   UNSAFE   OUTCOME_UNKNOWN  1         <ts>        2
1 unsettled call
```

```
$ conductor inspect conversation 7b3c1f2a --from-step 4
step  gen  state     kind         summary
4     1    PENDING   tool_call    charge_card call_id=9f2a...
5     1    PENDING   tool_result  charge_card ok
6     2    PENDING   tool_call    send_email  call_id=4d81...
7     2    PENDING   content      "Charged and emailed..."
8     2    COMPLETED -            turn complete
```

### `conductor migrate`

Thin wrapper over `golang-migrate` against `db/migrations/`.

```
conductor migrate up      [--dsn <postgres-dsn>] [--steps N]
conductor migrate down    [--dsn <postgres-dsn>] --steps N
conductor migrate version [--dsn <postgres-dsn>]
```

```
$ conductor migrate up
applied 0001_generation_column
applied 0002_conversation_leases
applied 0003_runs_projection
applied 0004_tool_calls
schema version <N> (dirty: false)
```

`serve` refuses to start if the schema version is not the one the binary expects,
and says which `migrate` command to run.

### `conductor chaos`

Drives one failure injection against a running stack. Used by `bench`; useful
alone for reproducing a single scenario.

```
conductor chaos run --class <class>
                    [--at <boundary>]       # e.g. tool_started:send_email
                    [--conversation <id>]
                    [--target controller|harness]
                    [--repeat N]
                    [--assert I1,I2,I3,I4,I5,I6,I7,I8]
conductor chaos list                        # classes and boundaries
```

Boundaries are the named crash points from `ARCHITECTURE.md` §8.1
(`model_started`, `model_completed`, `tool_planned`, `tool_started`,
`tool_effect_done`, `tool_settled`, `event_appended`, `lease_renewed`). A hook
at the chosen boundary aborts the process rather than unwinding, so the failure
resembles a real kill.

```
$ conductor chaos run --class controller_kill --at tool_started:send_email --repeat 5
class=controller_kill at=tool_started:send_email repeat=5
  run 1/5  killed at <ts>  recovered in <...>ms  invariants ok
  ...
recovered <N>/5   invariant violations: <N>
```

### `conductor bench`

Load plus the full failure matrix, emitting the report in §8.

```
conductor bench --concurrency N --turns N --duration <d>
                [--harness fake|<id>]
                [--classes <list>]
                [--out bench/results/<timestamp>/]
```

Invoked by `make bench`.

---

## 6. Reconnect and resume: the user-visible contract

### 6.1 Two independent things, shipped at different times

| | What it covers | When |
|---|---|---|
| **Execution recovery** | The *work* survives. A crashed, evicted, or de-owned run is picked up by another controller and driven to a terminal state. | **P0** |
| **Client-stream catch-up replay** | The *view* survives. A reconnecting client is sent the steps it missed before being joined to the live stream. | **P1** |

They are separable because the log is the source of truth: the work completing
and the client seeing it are different problems. Recovery is the one with
correctness consequences, so it is first.

**What the user experiences in the interim (P0):** a disconnect loses the
stream, not the run. The client exits non-zero. The run continues or is
recovered. To see what happened, read the log:
`conductor inspect conversation <id>`. To re-attach, `conductor run
--conversation <id> --resume` — which joins the live stream from the current
point and shows nothing that happened while you were away. Nothing is lost; it
is just not streamed to you. This is documented in `conductor run --help`, not
buried.

One sharp edge to know about: if you reconnect while the previous server-side
turn is still unwinding, you get gRPC `FAILED_PRECONDITION` with
`conversation <id> is already in flight`. Retry with backoff. This is the
in-process guard in `internal/server/server.go:62-65` and it is intentionally
conservative.

### 6.2 Four failure cases

**The client disconnects (`Ctrl-C`, network drop, pod eviction of the caller).**
The run keeps going. The controller that owns it holds a valid lease and keeps
executing to the end of the current turn. If the run needs no further input it
reaches a terminal state with no client attached. Nothing is written to a client
that is gone; nothing is lost. P0: no catch-up on reconnect. P1: catch-up via
`EventsSince(conversation_id, after_step)`.

**The controller dies mid-tool-call.** The hard case, and the reason the project
exists. Order of events:

1. The tool call was durably recorded as `PLANNED` with a Conductor-computed
   `call_id` and `idempotency_key` before anything external happened.
2. It was durably recorded as `STARTED` before the external call.
3. The process dies. The lease expires (or is released, on `SIGTERM`).
4. Another controller acquires the lease, bumping the conversation's fencing
   generation. Any write the dead process somehow still issues is rejected by
   the persistence layer and counted in `conductor_fenced_writes_rejected_total`.
5. Every call still in `STARTED` becomes `OUTCOME_UNKNOWN` — a durable resting
   state, not an in-memory one, so a second crash during resolution loses
   nothing.
6. Each `OUTCOME_UNKNOWN` call is resolved by its declared class (§6.3).
7. The run resumes from state rebuilt from the log.

What you can determine afterwards, from the database alone: every call, its
arguments, its idempotency key, how many attempts it had, and its terminal
state. That is the guarantee. It is not "the side effect happened exactly once".

**The executor pod is deleted.** The controller notices the harness connection
drop, the turn fails, and the run returns to the recovery path above. The new
owner labels its execution environments with its generation and removes
environments from lower generations before starting its own. Note the honest
limit: deleting a pod is asynchronous, and a terminating pod's container keeps
running through its grace period. Deletion reclaims resources; it does not fence
effects. **Fencing is done by the ledger, not by the pod.** The guarantee is "at
most one *committing* writer per conversation", not "at most one *running*
process".

**Ownership moves (rolling update, scale-down, failover).** On `SIGTERM` the
controller releases its leases explicitly, so a planned handover is immediate
rather than waiting out the lease TTL. On an unplanned loss the new owner waits
for the TTL. Two numbers are therefore reported separately in §8: graceful
handover latency and ungraceful-kill recovery latency. Blending them would
flatter the graceful case and hide the real one.

### 6.3 Tool recovery policy

Declared per tool in config. Three classes, three behaviours, no policy engine.

| `class` | Meaning | On `OUTCOME_UNKNOWN` |
|---|---|---|
| `IDEMPOTENT` | Read-only, or safe to repeat with the same key | Re-execute with the same `idempotency_key`; `attempts` increments |
| `RECONCILABLE` | Not safely repeatable, but external state is queryable | Query the external system and settle `SUCCEEDED` or `FAILED` |
| `UNSAFE` | Neither repeatable nor queryable | Settle `ABANDONED` and fail the run, with the uncertainty recorded |

`ABANDONED` counts as settled, so a run never hangs forever waiting on an
uncertainty nothing can resolve. Reconciliation attempts are bounded; a
`RECONCILABLE` call whose external system stays unreachable eventually becomes
`ABANDONED` rather than sitting unsettled indefinitely.

### 6.4 Backends and ownership

| Backend | Use | Ownership |
|---|---|---|
| PostgreSQL | production, multi-replica | durable fenced lease |
| SQLite | single-process local development only | **none** — process-local |

SQLite is supported for `conductor run` on a laptop and nothing else. On SQLite,
`serve` logs a prominent warning, `doctor` reports
`ownership=process-local`, and starting with multi-replica configuration is
refused outright rather than degraded. Every correctness test in
`docs/conductor/correctness.md` runs against PostgreSQL only, and the benchmark
reports the backend it ran on.

### 6.5 Which harnesses are recoverable

| Harness | Owns tool loop | Recoverable | Notes |
|---|---|---|---|
| `fake` | Conductor | yes | deterministic, zero-key, source of all benchmark numbers |
| `openai-compat` | Conductor | yes | any OpenAI-compatible endpoint with tool calling |
| `antigravity` | harness | no | state in a local sidecar |
| `antigravity-interactions` | harness | no | conversation state in a remote service, keyed by an interaction id Conductor does not hold |
| `substrate` | harness | no | requires Agent Substrate |

Recovery requires that Conductor, not the harness, decides when each tool runs
and records it before and after. The upstream harnesses execute tools internally
and keep conversation state outside the log, so Conductor cannot reconstruct
them; the recovery loop skips their conversations and `doctor` reports
`recovery: none`. This is a real constraint on what "crash-recoverable" means
here, and it is stated rather than papered over.

---

## 7. Milestones

Blocks are `PLAN.md` §3's six. Estimates assume one engineer at **10–12 hours
per week**, and include tests, docs, and CI for each block.

| M | Block | Deliverable | Definition of Done | Estimate |
|---|---|---|---|---|
| M0 | — | Fork, pin, read the streaming path end to end | A written walkthrough of `internal/controller` + `internal/server` (~950 LOC) exists and is accurate | `<1 week>` |
| M1 | ① Bootstrappable | Generic remote `HarnessService` client (~80 LOC, extracted from logic duplicated in `substrate.go:194-233` and `antigravity.go:163-200`); deterministic `fake` harness with its `fake_effects` table; **`openai-compat` harness**; `docker compose` stack; migrations | `git clone && docker compose up` brings up PostgreSQL + Conductor + fake harness + Prometheus + Grafana; a full execution runs end to end with **zero API keys**; pointing `OPENAI_BASE_URL` at a local model runs the same agent; the same path runs in CI | `<4–5 weeks>` |
| M2 | — | `docs/conductor/correctness.md` | Every invariant stated so that it names the SQL query or test that falsifies it | `<3 days>` |
| M3 | ② Effect log | Tool call/result reach the event log; durable ledger with stable `call_id`, idempotency key, attempts, `OUTCOME_UNKNOWN` | After any injected crash, the durable state alone reconstructs which tools were called, with what arguments, what each returned, how many attempts, and each terminal state; the `OUTCOME_UNKNOWN` path has tests | `<3–4 weeks>` |
| M4 | ② Recovery | Stop swallowing append errors (`controller.go:153-162`) and add `event_id` so the resulting retry is safe (I9, I10) — **this precedes the reconciler**; then rebuild execution state from the log, `Queue()` before `Run()` on `PENDING`, poison guard, terminal-state short circuit | `conductor run --conversation <id> --resume` works (it cannot today, for any harness); a killed controller's run is picked up and completed by another | `<2–3 weeks>` |
| M5 | ⑤a Operability (part 1) | Structured logs with `conversation_id` / `trace_id` / `owner_id` / `step`; the core metric set | Every metric in `ARCHITECTURE.md` §7.2 that does not depend on later blocks is emitted and scraped | `<1–2 weeks>` |
| M6 | ③ Executor | `Executor` abstraction; `local` backend, then the narrowest `pod` backend. No Substrate adapter, no PDB, no resource tuning, no gVisor | On `kind`, with Agent Substrate absent, deleting the executor pod lets the session recover and complete. One assertion | `<3–5 weeks>` |
| M7 | ④ Ownership | PostgreSQL lease + fencing generation; fenced appends; multi-replica | Three controller replicas; killing the holder transfers ownership; a write from the stale holder carrying an expired generation is rejected by the persistence layer; a rolling update produces no duplicate terminal state (this assertion moved here from M6 — it is about controller replicas and leases, not pods) | `<3–4 weeks>` |
| M8 | ⑤b Operability (part 2) | Full-path tracing; checked-in Grafana dashboard | One trace spans client → server → controller → executor → model → tool → append; a panel answers "how long did the last recovery take, and how many tool attempts did it involve" | `<2 weeks>` |
| M9 | ⑥ Chaos + bench | Failure injection harness; load driver; invariant assertions; `make bench` | Each failure class × N injections; every invariant asserted individually, not "did it recover"; reproducible with one command | `<4–5 weeks>` |
| P1 | — | Resumable client stream · Anthropic adapter · gVisor `RuntimeClass` · log snapshot/compaction | each with its own DoD when scheduled | not estimated |

**Total: roughly `<24–32 weeks>`, i.e. six to eight months at 10–12 h/week.**

**Stated plainly: as a single sequential plan this is not a short project, and
the risk is not that it fails but that it stalls at M6.** M6 introduces the
module's first Kubernetes client dependency (there is none today — `go.sum`
contains zero `k8s.io/` entries), plus informer lifecycle, RBAC, pod-to-pod
addressing, image publishing, and a `kind` CI job. It is the largest block, not
the smallest.

**Ordering, revised:** M7 (ownership) is implemented **before** M6 (executor).
Ownership is roughly 200 lines against a database and carries the
highest-density claim in the project; the executor is the largest block and the
likeliest place to stall. If the schedule gives, it gives at M6 — cut to one
backend and one assertion — never at M3, M4, M7 or M9, which is where the
correctness signal lives.

**A shorter honest stopping point:** M0–M4 plus M7 (≈`<14–19 weeks>`) delivers a
complete, defensible project: a complete effect log, event-sourced recovery, a
durable tool ledger with uncertain-outcome reconciliation, and fenced
multi-replica ownership — all on the `local` executor, all measurable. M6 adds
Kubernetes depth and M9 adds the benchmark table. Anyone planning around a
deadline should treat M4 and M7 as the deliverable and M6/M9 as extensions.

A note on where the numbers come from: all of M9's load and recovery numbers
come from the `fake` harness, because real model calls are nondeterministic,
rate-limited, and cost money. That is the right engineering choice. It is also
why `openai-compat` was moved **out of P1 and into M1**: recovery can only close
the loop for a harness whose tool loop Conductor owns (§6.5), so `fake` and
`openai-compat` are the only recoverable harnesses, and shipping the second one
early is what makes the recovery story verifiable against a real model rather
than only against a harness this project wrote itself. Real-model runs are
reported separately from the deterministic table, never mixed into it.

---

## 8. `make bench` output contract

### 8.1 Rules

- All load and recovery numbers come from the deterministic `fake` harness.
  Real-model runs may be reported, but separately and never in the same table.
- Every invariant is asserted individually, with the check that was run named.
  "It recovered" is not an assertion.
- Duplicate effects are detected mechanically, by SQL over an append-only
  effects table the fake harness writes, not by inspection.
- `make bench` writes both a human table to stdout and `results.json` to
  `bench/results/<timestamp>/`, so results are diffable across commits.
- Nothing below has been measured. Every `<...>` is a placeholder.

### 8.2 Table shape

```
Conductor benchmark
  commit        <git-sha>              dirty: <yes/no>
  started       <rfc3339>
  duration      <...>
  backend       postgres <version>
  replicas      <N>
  executor      <local|pod>
  harness       fake (deterministic)
  load          <C> concurrent sessions, <T> turns/session, <K> tool calls/turn

LOAD
  metric                                   value
  ------------------------------------------------
  sessions started                         <int>
  sessions completed                       <int>
  sessions failed                          <int>
  max concurrent sessions sustained        <int>
  throughput                               <float> sessions/min
  turn latency p50 / p95 / p99             <int> / <int> / <int> ms
  run latency  p50 / p95 / p99             <int> / <int> / <int> ms
  error rate                               <float> %

RECOVERY  (by failure class)
  class                       inj.   recovered   rate     p50      p95      p99
  --------------------------------------------------------------------------------
  controller_kill_graceful    <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  controller_kill_sigkill     <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  harness_kill                <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  db_unavailable_on_append    <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  duplicate_concurrent_req    <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  rolling_restart             <int>  <int>       <float>% <int>ms  <int>ms  <int>ms
  --------------------------------------------------------------------------------
  TOTAL                       <int>  <int>       <float>% <int>ms  <int>ms  <int>ms

  recovery abandoned (attempt cap reached)  <int>

INVARIANTS
  id    assertion                                                checks   viol.
  --------------------------------------------------------------------------------
  I1    every STARTED call reached a settled state               <int>    <int>
  I1b   no run COMPLETED with an unsettled call                  <int>    <int>
  I2    no extra effect per idempotency_key (IDEMPOTENT)         <int>    <int>
  I3'   generation non-decreasing by step, per conversation      <int>    <int>
  I4    stale-generation write rejected                          <int>    <int>
  I5    conversation_log.step contiguous from 1                   <int>    <int>
  I6    rebuild(log) == post-recovery state                      <int>    <int>
  I7a   runs projection == rebuild from log                      <int>    <int>
  I7b   tool_calls terminal states consistent with log           <int>    <int>
  I8    no Conductor-owned UNSAFE start inside lease margin      <int>    <int>
  --------------------------------------------------------------------------------
  TOTAL                                                          <int>    <int>

TOOL LEDGER
  tool            class          calls  attempts  unknown  reconciled  abandoned
  --------------------------------------------------------------------------------
  <name>          IDEMPOTENT     <int>  <int>     <int>    <int>       <int>
  <name>          RECONCILABLE   <int>  <int>     <int>    <int>       <int>
  <name>          UNSAFE         <int>  <int>     <int>    <int>       <int>

OWNERSHIP
  lease acquisitions (success / contended)   <int> / <int>
  lease losses by reason                     renew_timeout=<int> deadline=<int> preempted=<int>
  fenced writes rejected                     <int>
  max generation observed                    <int>

DUPLICATE EFFECTS
  IDEMPOTENT tools, extra effects per idempotency_key   <int>
  (SQL: SELECT count(*) FROM (SELECT idempotency_key FROM fake_effects
        GROUP BY idempotency_key HAVING count(*) > 1) d)

EVENT LOG
  events appended                            <int>
  events replayed during recovery            <int>
  replay events per recovery p95             <int>
  event loss detected                        <int>

RESULT: <PASS|FAIL>   (<int> invariant violations, <int> duplicate effects)
```

`RESULT` is `PASS` only when invariant violations and duplicate effects are both
zero. Recovery rate below 100% is reported, not hidden, and does not by itself
fail the run — a `RECONCILABLE` call whose fake external system was deliberately
kept unreachable is a correct `ABANDONED`, not a bug.

---

## 9. Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| Recovery is only real for harnesses Conductor owns the tool loop for | The headline claim is narrower than "crash-recoverable agent runtime" sounds | Stated in §1 and §6.5; `doctor` reports per-harness recoverability; the recovery loop skips non-recoverable conversations instead of half-recovering them |
| All benchmark numbers come from a harness this project wrote | A reader can dismiss them as self-dealing | Documented in §8.1; the fake harness and its scripts are in the repo and re-runnable; `openai-compat` (M1) is the path to real-model numbers, reported separately |
| `UNSAFE` tool outcomes are genuinely unresolvable | Users may expect the runtime to fix this | `UNSAFE` is a declared property of *their* tool, not a Conductor behaviour. Conductor's contribution is recording the uncertainty durably and refusing to pretend otherwise |
| Full-history replay cost grows with conversation length | A long conversation gets slower to recover; the current query has no `LIMIT`, no cursor, and no snapshot | `conductor_replay_events_total` makes the growth visible before it hurts; `EventsSince` supports incremental replay; snapshot/compaction is P1 and explicitly deferred, not forgotten |
| Lease TTL tuning trades failover speed against false preemption | Too short preempts healthy owners during GC pauses; too long stalls sessions after a hard kill | Graceful shutdown releases leases so planned handover does not wait; `conductor_lease_losses_total{reason}` shows whether real losses are timeouts or preemptions; TTL is a flag |
| Clock behaviour between replicas | A lease decided partly by DB time and partly by wall-clock time is wrong by the skew | All lease transitions are decided by single SQL statements using the database's `now()`; the client converts the returned remaining TTL into a local monotonic deadline and never compares a database timestamp to its own clock |
| A conversation that deterministically crashes recovery | A poison run can cycle through every replica and wedge the cluster | Recovery attempts are counted durably before replay, with a cap after which the run is failed with an explicit reason; attempted runs are backed off in the candidate query |
| Upstream protocol is mid-redesign | `proto/ax.proto` carries `TODO(jbd): CreateInteraction should return an Interaction message`; `CreateInteractionEvent` has `reserved 3` where `last_seq` used to be | The fork is pinned at `b777313` and does not track `main`; cherry-picks are deliberate, never opportunistic |
| Kubernetes support brings a large new dependency tree | `client-go` and its transitive modules are not in the module today; build time, binary size, and CVE surface all grow | The executor abstraction keeps the Kubernetes backend behind one interface with a subprocess backend that satisfies the same tests; if the cost is not worth it, the pod backend can be dropped without touching recovery or ownership |
| SQLite looks production-ready and is not | Someone deploys multi-replica on SQLite and files a correctness bug against the central claim | No lease implementation on SQLite at all; a startup refusal rather than a degraded mode; `doctor` says `ownership=process-local` |
| Fork attribution | Apache-2.0 requires retaining notices and stating modifications | README names the fork, the pinned upstream commit, what upstream provides, and what Conductor adds; upstream subsystems are never presented as originally authored here |

---

## 10. Non-goals

Conductor is a *runtime*. It does not try to be the agent, the model, or the
model gateway. If you need any of the following, Conductor is not where it lives:

- **Model or provider breadth.** One OpenAI-compatible harness, plus a native
  Anthropic adapter in P1. No routing, no fallback chains, no cost optimisation,
  no provider matrix.
- **Prompting, evaluation, or training.** No prompt templates, no golden sets,
  no scoring, no fine-tuning or SFT. Conductor does not know whether the agent
  gave a good answer.
- **Retrieval or memory.** No RAG, no vector store, no long-term agent memory.
- **Multi-agent orchestration or planning.** One conversation, one agent, one
  tool loop.
- **Durable human-in-the-loop approvals.** The approval message types exist in
  the inherited protocol (`proto/content.proto`), but there is no durable
  `WAITING_FOR_APPROVAL` state; the only consumer is the interactive CLI loop
  and it blocks in-process. If your CLI dies, a pending approval is lost. This
  is a known gap, not a feature in progress.
- **Sandboxing or isolation guarantees.** Tools run with the privileges of the
  harness process. gVisor `RuntimeClass` support is P1, and even then isolation
  is the cluster's property, not Conductor's.
- **A Kubernetes operator or CRD.** The executor owns Pod lifecycle — create,
  wait ready, connect, drain, delete. It is not a controller-runtime operator,
  it defines no custom resources, and it does not schedule or autoscale.
- **A stable Go API.** The code lives under `internal/`. The supported extension
  boundary is the gRPC `HarnessService`, not Go imports. Expect Go-level
  breaking changes without notice.
- **Voice, multimodal input, or any frontend.** There is a CLI and a gRPC API.
- **Exactly-once external side effects.** Not deferred — refused. See §1.
```

---

