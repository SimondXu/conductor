# Copyright 2026 Google LLC
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

# Multi-target Conductor image build:
#   --target antigravity : python:3.13-slim + the Antigravity Python sidecar
#                          (SDK, localharness, agent). Used by the antigravity
#                          harness actor (`conductor harness antigravity`).
#   --target conductor   : debian:stable-slim + the Go `conductor` binary. Used by
#                          the controller (`conductor serve`) and the Go interactions
#                          harness actor (`conductor harness antigravity-interactions`).
#
# Build context: repository root. Images target the cluster's linux/amd64 nodes,
# so build with `--platform linux/amd64`.
# For a standalone build:
#   docker build --platform linux/amd64 --target antigravity -f cmd/conductor/Dockerfile -t <ref> . && docker push <ref>
#   docker build --platform linux/amd64 --target conductor -f cmd/conductor/Dockerfile -t <ref> . && docker push <ref>

# --- Stage 1: build the conductor Go binary ----------------------------------
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -o /out/conductor ./cmd/conductor

# --- antigravity image -------
FROM python:3.13-slim AS antigravity
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        less \
        procps \
        wget \
    && rm -rf /var/lib/apt/lists/*

# Antigravity runtime deps, installed from PyPI at build time.
COPY python/antigravity/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Python sidecar (with generated proto stubs), the agent it serves, and the
# conductor binary built in stage 1.
COPY python/ /conductor-app/python

# You can copy skills or use skills managers (e.g. npx skills)
# to download them into the image.
# Example: COPY examples/skills/ /ax/skills/

ENV SKILLS_DIR=/ax/skills
COPY --from=build /out/conductor /conductor-app/conductor

# `from python.proto import ...` needs /conductor-app on the path; the generated stubs do
# `from proto import ...`, which needs /conductor-app/python.
ENV PYTHONPATH=/conductor-app:/conductor-app/python

# Flush stdout/stderr immediately so server logs surface in container logs.
ENV PYTHONUNBUFFERED=1

EXPOSE 80

# Default command for local docker runs. Substrate ignores the image CMD and runs
# the ActorTemplate `command` instead. `conductor harness` forks the Python sidecar,
# which serves the HarnessService on :80.
CMD ["/conductor-app/conductor", "harness", \
     "--host", "0.0.0.0", "--port", "80"]

# --- conductor image (Go-only) for the interactions harness and controller ---
FROM debian:stable-slim AS conductor
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        less \
        procps \
        wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/conductor /conductor-app/conductor
ENV PATH=/conductor-app:$PATH

EXPOSE 80

CMD ["/conductor-app/conductor", "harness", "antigravity-interactions", \
     "--host", "0.0.0.0", "--port", "80"]
