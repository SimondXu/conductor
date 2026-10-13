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
#
# GVISOR=1 (P1) first installs gVisor's runsc on the node (GVISOR_RELEASE,
# default 20260928, sha512-checked), runs the harness pods with
# runtime_class: gvisor, and also asserts that the harness pod's kernel is
# gVisor's.
set -euo pipefail
cd "$(dirname "$0")/../.."

gvisor=${GVISOR:-}
cluster=${KIND_CLUSTER:-conductor-e2e${gvisor:+-gvisor}}
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

# install_runsc puts runsc, its containerd shim and the sidecars runsc runs
# from gvisor-bin/ next to it on the node, and registers the runtime handler
# runsc, once. gVisor publishes each release as one tarball; it is fetched
# and checked on this host, which has bzip2, and runsc's directory is copied
# into the node's /usr/local/bin.
install_runsc() {
  local node="$cluster-control-plane"
  if ! docker exec "$node" test -x /usr/local/bin/runsc; then
    local tmp url
    tmp=$(mktemp -d)
    url="https://storage.googleapis.com/gvisor/releases/release/${GVISOR_RELEASE:-20260928}/$(docker exec "$node" uname -m)/gvisor.tar.bz2"
    curl -fsSL -o "$tmp/gvisor.tar.bz2" "$url"
    curl -fsSL -o "$tmp/gvisor.tar.bz2.sha512" "$url.sha512"
    (cd "$tmp" && shasum -a 512 -c gvisor.tar.bz2.sha512)
    tar -xjf "$tmp/gvisor.tar.bz2" -C "$tmp"
    rm "$tmp/gvisor.tar.bz2" "$tmp/gvisor.tar.bz2.sha512"
    local dir shim
    dir=$(dirname "$(find "$tmp" -type f -name runsc | head -1)")
    shim=$(find "$tmp" -type f -name containerd-shim-runsc-v1 | head -1)
    [[ -x "$dir/runsc" && -n "$shim" ]] || fail "no runsc or containerd-shim-runsc-v1 in $url"
    docker cp "$dir/." "$node:/usr/local/bin/"
    docker cp "$shim" "$node:/usr/local/bin/containerd-shim-runsc-v1"
    rm -rf "$tmp"
    docker exec "$node" sh -ec 'chmod +x /usr/local/bin/runsc /usr/local/bin/containerd-shim-runsc-v1
      printf "%s\n" "[plugins.\"io.containerd.grpc.v1.cri\".containerd.runtimes.runsc]" "  runtime_type = \"io.containerd.runsc.v1\"" >> /etc/containerd/config.toml
      systemctl restart containerd'
    kubectl --context "kind-$cluster" wait --for=condition=Ready node --all --timeout 120s
  fi
  docker exec "$node" runsc --version | head -1
}

kind get clusters | grep -qx "$cluster" || kind create cluster --name "$cluster" --wait 120s
[[ -z "$gvisor" ]] || install_runsc
docker build -t conductor:dev -f cmd/conductor/Dockerfile --target conductor .
kind load docker-image conductor:dev --name "$cluster"
if [[ -n "$gvisor" ]]; then
  kubectl --context "kind-$cluster" apply -f deploy/k8s/gvisor.yaml
  awk '{print} /grace_period_seconds: 2/ {print "          runtime_class: gvisor"}' deploy/k8s/conductor.yaml |
    kubectl --context "kind-$cluster" apply -f -
else
  kubectl --context "kind-$cluster" apply -f deploy/k8s/conductor.yaml
fi
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
if [[ -n "$gvisor" ]]; then
  [[ "$(k get "$pod" -o jsonpath='{.spec.runtimeClassName}')" == "gvisor" ]] || fail "$pod has no runtimeClassName gvisor"
  # The kernel the harness sees: gVisor's own, not the node's. Its dmesg
  # names gVisor, and its /proc/version is a fixed string dated
  # "Sun Jan 10 15:06:54 PST 2016" ("Linux version 4.19.0-gvisor" in
  # release-20260928.0).
  version=$(k exec "$pod" -- cat /proc/version)
  { k exec "$pod" -- dmesg 2>/dev/null | grep -q gVisor || [[ "$version" == *"Sun Jan 10 15:06:54 PST 2016"* ]]; } ||
    fail "$pod is not running under gVisor: $version"
  echo "$pod runs under gVisor: $version"
fi
k delete "$pod" --wait=false
wait "$client" || true # the client sees the harness go away

until_eq 120 "SELECT state FROM runs WHERE conversation_id = '$conv'" "COMPLETED"
call=$(psql "SELECT state || ' ' || attempts || ' gen ' || generation FROM tool_calls WHERE conversation_id = '$conv'")
echo "PASS: $conv completed after its harness pod ($pod) was deleted mid-tool-call; call: $call"

# Rolling update (M7 DoD, on Kubernetes): two controller replicas, a run of
# six slow IDEMPOTENT tool turns, and `kubectl rollout restart` while it is
# in flight. Each old pod drains (SIGTERM, then the drain timeout), cancels
# the run and releases its lease; another replica takes it over. The run
# must complete once, every call once in effect, at a higher generation,
# with every controller pod replaced.
k scale deploy/conductor --replicas 2
k rollout status deploy/conductor --timeout 180s
before=$(k get pods -l app=conductor -o jsonpath='{.items[*].metadata.name}')
conv="rolling-update-$(date +%s)"
turn='{"tool_calls":[{"name":"charge_card","args":{"n":N}}]}'
turns=$(for n in 1 2 3 4 5 6; do printf '%s,' "${turn/N/$n}"; done)
cfg="{\"turns\":[${turns}{\"content\":\"Charged six times.\"}],\"tools\":{\"charge_card\":{\"idempotent\":true,\"delay_ms\":8000}}}"
k exec deploy/conductor -- conductor run --server localhost:8494 --conversation "$conv" \
  --agent-config "$cfg" --input "charge me six times" >/dev/null 2>&1 &
client=$!
until_eq 120 "SELECT count(*) FROM tool_calls WHERE conversation_id = '$conv' AND state = 'STARTED'" "1"
k rollout restart deploy/conductor
k rollout status deploy/conductor --timeout 300s
wait "$client" || true # the client's pod is one of those replaced

until_eq 300 "SELECT state FROM runs WHERE conversation_id = '$conv'" "COMPLETED"
after=$(k get pods -l app=conductor --field-selector status.phase=Running -o jsonpath='{.items[*].metadata.name}')
for p in $before; do
  [[ " $after " != *" $p "* ]] || fail "pod $p survived the rolling update"
done
terminal=$(psql "SELECT count(*) FROM conversation_log WHERE conversation_id = '$conv'
                   AND payload::jsonb->>'state' IN ('STATE_COMPLETED', 'STATE_FAILED', 'STATE_CANCELED')")
[[ "$terminal" == "1" ]] || fail "$conv has $terminal terminal events, want 1"
calls=$(psql "SELECT count(*) FILTER (WHERE state = 'SUCCEEDED') || '/' || count(*) FROM tool_calls WHERE conversation_id = '$conv'")
[[ "$calls" == "6/6" ]] || fail "$conv: $calls calls SUCCEEDED, want 6/6"
effects=$(psql "SELECT count(*) FROM fake_effects e JOIN tool_calls t USING (call_id) WHERE t.conversation_id = '$conv'")
[[ "$effects" == "6" ]] || fail "$conv has $effects effects, want 6 (one per call)"
dups=$(psql "SELECT count(*) FROM (SELECT e.idempotency_key FROM fake_effects e JOIN tool_calls t USING (call_id)
               WHERE t.conversation_id = '$conv' GROUP BY 1 HAVING count(*) > 1) d")
[[ "$dups" == "0" ]] || fail "$conv: $dups idempotency keys with more than one effect (I2)"
gap=$(psql "SELECT count(*) <> max(step) OR min(step) <> 1 FROM conversation_log WHERE conversation_id = '$conv'")
[[ "$gap" == "f" ]] || fail "$conv: conversation_log steps are not contiguous from 1 (I5)"
gen=$(psql "SELECT max(generation) FROM tool_calls WHERE conversation_id = '$conv'")
(( gen > 1 )) || fail "$conv: max generation $gen, want > 1 (the run never changed owner)"
attempts=$(psql "SELECT sum(attempts) FROM tool_calls WHERE conversation_id = '$conv'")
echo "PASS: $conv completed once across a rolling update of 2 replicas; 6 calls, $attempts attempts, $effects effects, max generation $gen"
