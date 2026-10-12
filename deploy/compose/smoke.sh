#!/usr/bin/env bash
# Copyright 2026 Simon Xu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end check of the compose stack with zero API keys: bring it up, run
# the fake script once through the server, and assert on Postgres and
# Prometheus. Used locally and in CI. Leaves the stack running.
set -euo pipefail
cd "$(dirname "$0")"

dc() { docker compose -f docker-compose.yml "$@"; }
psql() { dc exec -T postgres psql -U conductor -d conductor -tAc "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

dc up -d --build --wait

conv="smoke-$(date +%s)"
dc exec -T conductor conductor run --server localhost:8494 --conversation "$conv" --input "book me a flight"

state=$(psql "SELECT payload::jsonb->>'state' FROM conversation_log WHERE conversation_id = '$conv' ORDER BY step DESC LIMIT 1")
[[ "$state" == "STATE_COMPLETED" ]] || fail "final state is '$state', want STATE_COMPLETED"

steps=$(psql "SELECT count(*) FROM conversation_log WHERE conversation_id = '$conv'")
[[ "$steps" == "8" ]] || fail "conversation has $steps log rows, want 8"

effects=$(psql "SELECT count(*) FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.conversation_id = '$conv'")
[[ "$effects" == "2" ]] || fail "fake_effects has $effects rows for $conv, want 2"

settled=$(psql "SELECT count(*) FROM tool_calls WHERE conversation_id = '$conv' AND state = 'SUCCEEDED' AND attempts = 1")
[[ "$settled" == "2" ]] || fail "tool_calls has $settled settled calls for $conv, want 2"

# M8: one trace spans client -> server -> controller -> harness -> append.
# Find the client's trace for $conv in Jaeger and require every hop in it.
# (The compose stack dials a fixed harness, so it has no executor.acquire.)
trace_ok=""
for _ in $(seq 1 30); do
  if curl -sf "http://localhost:${JAEGER_PORT:-16686}/api/traces?service=conductor-cli&limit=50" | python3 -c '
import json, sys
conv = sys.argv[1]
want_services = {"conductor-cli", "conductor-server", "fake-harness"}
want_ops = {"conductor.run", "exec", "lease.acquire", "replay", "harness.turn", "model.call", "tool.execute", "eventlog.append"}
for t in json.load(sys.stdin)["data"]:
    if not any(tag["key"] == "conversation_id" and tag["value"] == conv for s in t["spans"] for tag in s["tags"]):
        continue
    services = {p["serviceName"] for p in t["processes"].values()}
    ops = {s["operationName"] for s in t["spans"]}
    missing = (want_services - services) | (want_ops - ops)
    sys.exit(1 if missing else 0)
sys.exit(1)' "$conv"; then
    trace_ok=1
    break
  fi
  sleep 1
done
[[ -n "$trace_ok" ]] || fail "no complete trace for $conv in Jaeger"
panels=$(curl -sf "http://localhost:${GRAFANA_PORT:-3000}/api/dashboards/uid/conductor-runtime" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["dashboard"]["panels"]))')
[[ "$panels" == "16" ]] || fail "Grafana dashboard has '$panels' panels, want 16"

# until <seconds> <sql> <want>: poll psql until it prints want.
until_eq() {
  local got
  for _ in $(seq 1 "$1"); do
    got=$(psql "$2")
    [[ "$got" == "$3" ]] && return 0
    sleep 1
  done
  fail "waited $1s for '$3' from: $2 (last: '$got')"
}
last_state() { echo "SELECT payload::jsonb->>'state' FROM conversation_log WHERE conversation_id = '$1' ORDER BY step DESC LIMIT 1"; }
call_of() { echo "SELECT state || ' ' || attempts FROM tool_calls WHERE conversation_id = '$1'"; }
effects_of() { echo "SELECT count(*) FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.conversation_id = '$1'"; }

# Harness crash (M4): the fake harness dies after charge_card's effect and
# before its result, and the client exits non-zero. With no client, the
# reconciler runs the IDEMPOTENT call again under the same key and completes
# the run with the logged agent_config. A reconciler pass that beats the
# harness restart is one more attempt with no effect, so attempts is >= 2.
crash="crash-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"crash_after_effect":true}}}'
if dc exec -T conductor conductor run --server localhost:8494 --conversation "$crash" --agent-config "$cfg" --input "charge me"; then
  fail "the crashing run succeeded"
fi
until_eq 60 "$(last_state "$crash")" "STATE_COMPLETED"
[[ "$(psql "$(call_of "$crash")")" =~ ^SUCCEEDED\ ([2-9]|[1-9][0-9]+)$ ]] || fail "$crash call is '$(psql "$(call_of "$crash")")', want SUCCEEDED after 2 or more attempts"
[[ "$(psql "$(effects_of "$crash")")" == "1" ]] || fail "$crash has $(psql "$(effects_of "$crash")") effects, want 1"

# Holder killed mid-tool-call (M4 and M7 DoD): SIGKILL the replica that
# owns the run while the call is STARTED, and do not restart it. With no client
# attached, another replica takes the run over once the dead lease expires and
# completes it at a higher generation.
killed="killed-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"delay_ms":5000}}}'
dc exec -d -T fake-harness conductor run --server conductor:8494 --conversation "$killed" --agent-config "$cfg" --input "charge me"
until_eq 30 "SELECT state FROM tool_calls WHERE conversation_id = '$killed'" "STARTED"
dc kill -s SIGKILL conductor
until_eq 90 "$(last_state "$killed")" "STATE_COMPLETED"
[[ "$(psql "$(call_of "$killed")")" == "SUCCEEDED 2" ]] || fail "$killed call is '$(psql "$(call_of "$killed")")', want 'SUCCEEDED 2'"
[[ "$(psql "$(effects_of "$killed")")" == "1" ]] || fail "$killed has $(psql "$(effects_of "$killed")") effects, want 1"
gen=$(psql "SELECT generation FROM tool_calls WHERE conversation_id = '$killed'")
(( gen > 1 )) || fail "$killed call settled at generation $gen, want > 1"
dc start conductor
dc up -d --wait conductor

# Rolling update (M7 DoD): SIGTERM the owner mid-tool-call. After its 2s drain
# it cancels the run and releases the lease, so another replica takes over at
# once, without waiting out the lease, and the run ends exactly once.
rolled="rolled-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"delay_ms":10000}}}'
dc exec -d -T fake-harness conductor run --server conductor:8494 --conversation "$rolled" --agent-config "$cfg" --input "charge me"
until_eq 30 "SELECT state FROM tool_calls WHERE conversation_id = '$rolled'" "STARTED"
dc stop -t 30 conductor
until_eq 60 "$(last_state "$rolled")" "STATE_COMPLETED"
dc logs conductor-2 conductor-3 | grep '"recovery.start"' | grep "\"$rolled\"" | grep -q '"kill_kind":"graceful"' \
  || fail "$rolled was not taken over after a graceful release"
[[ "$(psql "$(effects_of "$rolled")")" == "1" ]] || fail "$rolled has $(psql "$(effects_of "$rolled")") effects, want 1"
dc start conductor
dc up -d --wait conductor

for c in "$crash" "$killed" "$rolled"; do
  n=$(psql "SELECT count(*) FROM conversation_log WHERE conversation_id = '$c'
              AND payload::jsonb->>'state' IN ('STATE_COMPLETED', 'STATE_FAILED', 'STATE_CANCELED')")
  [[ "$n" == "1" ]] || fail "$c has $n terminal events, want 1"
done

# Falsifying queries from docs/conductor/correctness.md; each must be empty.
check() {
  local rows
  rows=$(psql "$2")
  [[ -z "$rows" ]] || fail "$1 violated: $rows"
}
check I1 "SELECT call_id FROM tool_calls WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN') AND reconcile_deadline < now()"
check I2 "SELECT e.idempotency_key FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.class = 'IDEMPOTENT' GROUP BY e.idempotency_key HAVING count(*) > 1"
check I1b "SELECT r.conversation_id FROM runs r JOIN tool_calls t USING (conversation_id)
  WHERE r.state = 'COMPLETED' AND t.state IN ('STARTED', 'OUTCOME_UNKNOWN')"
check "I3'" "SELECT conversation_id FROM (SELECT conversation_id, generation,
  lag(generation) OVER (PARTITION BY conversation_id ORDER BY step) AS prev
  FROM conversation_log WHERE generation IS NOT NULL) s WHERE prev IS NOT NULL AND generation < prev"
check I7b "SELECT t.call_id FROM tool_calls t WHERE t.state = 'SUCCEEDED' AND NOT EXISTS (
  SELECT 1 FROM conversation_log l WHERE l.conversation_id = t.conversation_id
     AND l.payload::jsonb @> jsonb_build_object('steps', jsonb_build_array(
           jsonb_build_object('tool_result', jsonb_build_object('call_id', t.call_id)))))"

# Prometheus scrapes every 5s; give it a few rounds to see the run.
for _ in $(seq 1 12); do
  runs=$(dc exec -T prometheus wget -qO- 'http://localhost:9090/api/v1/query?query=sum(conductor_runs_total{outcome="completed"})' \
    | sed -n 's/.*"value":\[[^,]*,"\([0-9.]*\)"\].*/\1/p')
  [[ -n "$runs" && "$runs" != "0" ]] && break
  sleep 5
done
[[ -n "$runs" && "$runs" != "0" ]] || fail "Prometheus has no completed runs"

# M5: the core metrics are scraped. Labelled metrics appear only once
# something increments them, so this lists the ones the runs above must have
# produced (losses, skips and duplicate effects should not have happened).
names=$(dc exec -T prometheus wget -qO- 'http://localhost:9090/api/v1/label/__name__/values')
for m in conductor_runs_active conductor_runs_total conductor_run_duration_seconds_bucket \
         conductor_recovery_total conductor_recovery_duration_seconds_bucket conductor_recovery_poisoned_total \
         conductor_replay_events_total conductor_lease_acquisitions_total \
         conductor_tool_calls_total conductor_tool_attempts_total conductor_tool_duration_seconds_bucket \
         conductor_tool_outcome_unknown_total conductor_tool_calls_unsettled; do
  [[ "$names" == *"\"$m\""* ]] || fail "Prometheus has no $m"
done
up=$(dc exec -T prometheus wget -qO- 'http://localhost:9090/api/v1/query?query=up{job="fake-harness"}' \
  | sed -n 's/.*"value":\[[^,]*,"\([0-9]\)"\].*/\1/p')
[[ "$up" == "1" ]] || fail "Prometheus is not scraping the fake harness"

echo "PASS: $conv completed with $steps log rows and $settled settled calls; $crash (harness crash), $killed (owner SIGKILLed) and $rolled (owner SIGTERMed) finished once by another replica with one effect each; I1, I1b, I2, I3', I7b hold; one trace spans client to append; Prometheus sees $runs completed run(s)"
