// Copyright 2026 Simon Xu
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package agent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/google/uuid"
)

// mockModel is a scripted Messages API. It answers the refund task one tool
// call per turn, each turn led by a thinking block, and it enforces the
// check the real API runs on thinking blocks: a block's signature is the hash
// of everything before it, so a history that changed since the block was
// made is refused with a 400, as the real API refuses it.
type mockModel struct{}

func (mockModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError(w, "invalid_request_error", "decoding request: "+err.Error())
		return
	}
	msgs := req.Messages
	names := map[string]string{} // tool_use id -> tool name
	answered := map[string]bool{}
	failed := ""
	for i, m := range msgs {
		blocks, _ := m["content"].([]any)
		for j, b := range blocks {
			block, _ := b.(map[string]any)
			switch block["type"] {
			case "thinking":
				if block["signature"] != signature(msgs[:i], m, j) {
					apiError(w, "invalid_request_error", fmt.Sprintf("messages.%d.content.%d: Invalid `signature` in `thinking` block. The block is bound to a different conversation.", i, j))
					return
				}
				if m["role"] == "assistant" && j == len(blocks)-1 && i == len(msgs)-1 {
					apiError(w, "invalid_request_error", "the final assistant message must not end with a thinking block")
					return
				}
			case "tool_use":
				names[fmt.Sprint(block["id"])] = fmt.Sprint(block["name"])
			case "tool_result":
				name := names[fmt.Sprint(block["tool_use_id"])]
				answered[name] = true
				if block["is_error"] == true {
					failed = name
				}
			}
		}
	}

	next := map[string]any{"role": "assistant", "content": []any{}}
	content := []any{map[string]any{"type": "thinking", "thinking": "", "signature": signature(msgs, next, 0)}}
	stop := "tool_use"
	use := func(name string, input map[string]any) {
		content = append(content, map[string]any{"type": "tool_use", "id": "toolu_" + uuid.NewString()[:8], "name": name, "input": input})
	}
	switch {
	case failed != "":
		content, stop = append(content, map[string]any{"type": "text", "text": failed + " failed; a person will follow up."}), "end_turn"
	case !answered["lookup_order"]:
		use("lookup_order", map[string]any{"order_id": "1042"})
	case !answered["refund_payment"]:
		use("refund_payment", map[string]any{"order_id": "1042", "amount_cents": 4200})
	case !answered["send_email"]:
		use("send_email", map[string]any{"to": "ana@example.com", "subject": "Refund for order 1042", "body": "We refunded $42.00."})
	default:
		content, stop = append(content, map[string]any{"type": "text", "text": "Refunded $42.00 and emailed the customer."}), "end_turn"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": "msg_" + uuid.NewString()[:8], "type": "message", "role": "assistant", "model": "mock",
		"content": content, "stop_reason": stop, "stop_details": nil,
		"usage": map[string]any{"input_tokens": 100, "output_tokens": 20},
	})
}

// signature hashes the messages before a block: msgs, then msg's first j
// blocks. It decodes and re-encodes, so key order and spacing do not count.
func signature(msgs []map[string]any, msg map[string]any, j int) string {
	blocks, _ := msg["content"].([]any)
	head := map[string]any{"role": msg["role"], "content": blocks[:j]}
	b, _ := json.Marshal(append(append([]map[string]any{}, msgs...), head))
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func apiError(w http.ResponseWriter, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": kind, "message": msg}})
}

// meter sits between Conductor and the model endpoint. It caps the number
// of requests, stops everything on 401, 403 or 429, and adds up the tokens
// each response reports.
type meter struct {
	proxy       *httputil.ReverseProxy
	maxRequests int

	mu                     sync.Mutex
	requests, errors       int
	inputTokens, outTokens int64
	cacheRead, cacheWrite  int64
	abort                  string
}

func newMeter(upstream string, maxRequests int) *meter {
	u, err := url.Parse(upstream)
	if err != nil {
		panic(err)
	}
	m := &meter{maxRequests: maxRequests}
	m.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(u)
			r.Out.Host = u.Host
		},
		ModifyResponse: m.observe,
	}
	return m
}

func (m *meter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	refuse := m.abort
	if refuse == "" && m.requests >= m.maxRequests {
		refuse = fmt.Sprintf("request budget of %d spent", m.maxRequests)
		m.abort = refuse
	}
	if refuse == "" {
		m.requests++
	}
	m.mu.Unlock()
	if refuse != "" {
		// 400, so the SDK does not retry it.
		apiError(w, "invalid_request_error", "e2e meter: "+refuse)
		return
	}
	m.proxy.ServeHTTP(w, r)
}

func (m *meter) observe(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	m.mu.Lock()
	defer m.mu.Unlock()
	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		if m.abort == "" {
			m.abort = fmt.Sprintf("model endpoint returned %s: %s", resp.Status, truncate(string(body), 300))
		}
	}
	if resp.StatusCode/100 != 2 {
		m.errors++
		return nil
	}
	var out struct {
		Usage struct {
			Input      int64 `json:"input_tokens"`
			Output     int64 `json:"output_tokens"`
			CacheRead  int64 `json:"cache_read_input_tokens"`
			CacheWrite int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &out) == nil {
		m.inputTokens += out.Usage.Input
		m.outTokens += out.Usage.Output
		m.cacheRead += out.Usage.CacheRead
		m.cacheWrite += out.Usage.CacheWrite
	}
	return nil
}

// aborted says why the run must stop, or "".
func (m *meter) aborted() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.abort
}

func (m *meter) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("%d model requests (%d errors); tokens: %d input, %d output, %d cache read, %d cache write",
		m.requests, m.errors, m.inputTokens, m.outTokens, m.cacheRead, m.cacheWrite)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
