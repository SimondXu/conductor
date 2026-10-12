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

# M6 DoD on kind, with no Agent Substrate: start a run whose tool takes 20s,
# delete its harness pod while the call is STARTED, and assert the run
# recovers and completes. One assertion. Reuses the cluster if it exists and
# leaves it running; KIND_CLUSTER names it.
set -euo pipefail
cd "$(dirname "$0")/../.."

cluster=${KIND_CLUSTER:-conductor-e2e}
k() { kubectl --context "kind-$cluster" -n conductor "$@"; }
psql() { k exec deploy/postgres -- psql -U conductor -d conductor -tAc "$1"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
until_eq() {
  local got
  for _ in $(seq 1 "$1"); do
    got=$(psql "$2" 2>/dev/null || true)
    [[ "$got" == "$3" ]] && return 0
    sleep 1
  done
  fail "waited $1s for '$3' from: $2 (last: '$got')"
}

kind get clusters | grep -qx "$cluster" || kind create cluster --name "$cluster" --wait 120s
docker build -t conductor:dev -f cmd/conductor/Dockerfile --target conductor .
kind load docker-image conductor:dev --name "$cluster"
kubectl --context "kind-$cluster" apply -f deploy/k8s/conductor.yaml
k rollout status deploy/postgres --timeout 180s
k rollout restart deploy/conductor # pick up a rebuilt conductor:dev
k rollout status deploy/conductor --timeout 180s

conv="pod-delete-$(date +%s)"
cfg='{"turns":[{"tool_calls":[{"name":"charge_card","args":{"amount":4200}}]},{"content":"Charged."}],"tools":{"charge_card":{"idempotent":true,"delay_ms":20000}}}'
k exec deploy/conductor -- conductor run --server localhost:8494 --conversation "$conv" \
  --agent-config "$cfg" --input "charge me" >/dev/null 2>&1 &
client=$!
until_eq 120 "SELECT state FROM tool_calls WHERE conversation_id = '$conv'" "STARTED"
pod=$(k get pods -l app.kubernetes.io/component=harness -o name)
[[ -n "$pod" ]] || fail "no harness pod for $conv"
k delete "$pod" --wait=false
wait "$client" || true # the client sees the harness go away

until_eq 120 "SELECT state FROM runs WHERE conversation_id = '$conv'" "COMPLETED"
call=$(psql "SELECT state || ' ' || attempts || ' gen ' || generation FROM tool_calls WHERE conversation_id = '$conv'")
echo "PASS: $conv completed after its harness pod ($pod) was deleted mid-tool-call; call: $call"
