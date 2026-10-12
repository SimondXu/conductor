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
# the run with the logged agent_config.
crash="crash-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"crash_after_effect":true}}}'
if dc exec -T conductor conductor run --server localhost:8494 --conversation "$crash" --agent-config "$cfg" --input "charge me"; then
  fail "the crashing run succeeded"
fi
until_eq 60 "$(last_state "$crash")" "STATE_COMPLETED"
[[ "$(psql "$(call_of "$crash")")" == "SUCCEEDED 2" ]] || fail "$crash call is '$(psql "$(call_of "$crash")")', want 'SUCCEEDED 2'"
[[ "$(psql "$(effects_of "$crash")")" == "1" ]] || fail "$crash has $(psql "$(effects_of "$crash")") effects, want 1"

# Controller killed mid-tool-call (M4 DoD): SIGKILL the controller while the
# call is STARTED, then start it again. With no client attached, the new
# process takes the run over once the dead one's lease expires and completes
# it at a higher generation.
killed="killed-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"delay_ms":5000}}}'
dc exec -d -T fake-harness conductor run --server conductor:8494 --conversation "$killed" --agent-config "$cfg" --input "charge me"
until_eq 30 "SELECT state FROM tool_calls WHERE conversation_id = '$killed'" "STARTED"
dc kill -s SIGKILL conductor
dc start conductor
until_eq 90 "$(last_state "$killed")" "STATE_COMPLETED"
[[ "$(psql "$(call_of "$killed")")" == "SUCCEEDED 2" ]] || fail "$killed call is '$(psql "$(call_of "$killed")")', want 'SUCCEEDED 2'"
[[ "$(psql "$(effects_of "$killed")")" == "1" ]] || fail "$killed has $(psql "$(effects_of "$killed")") effects, want 1"
gen=$(psql "SELECT generation FROM tool_calls WHERE conversation_id = '$killed'")
(( gen > 1 )) || fail "$killed call settled at generation $gen, want > 1"

# Falsifying queries from docs/conductor/correctness.md; each must be empty.
check() {
  local rows
  rows=$(psql "$2")
  [[ -z "$rows" ]] || fail "$1 violated: $rows"
}
check I1 "SELECT call_id FROM tool_calls WHERE state IN ('STARTED', 'OUTCOME_UNKNOWN') AND reconcile_deadline < now()"
check I2 "SELECT e.idempotency_key FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.class = 'IDEMPOTENT' GROUP BY e.idempotency_key HAVING count(*) > 1"
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

echo "PASS: $conv completed with $steps log rows and $settled settled calls; $crash (harness crash) and $killed (controller SIGKILL) recovered by the reconciler with one effect over two attempts; I1, I2, I7b hold; Prometheus sees $runs completed run(s)"
