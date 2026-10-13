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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// mockModel is a scripted Messages API. It does the task in the latest user
// message one tool call per turn, each turn led by a thinking block: look the
// order up, refund it, email the customer, answer. A tool that returns an
// error is called once more, then given up on. It enforces two checks the
// real API runs: a thinking block's signature is the hash of everything
// before it, so a history that changed since the block was made is refused
// with a 400; and every tool_use is answered by a tool_result at the start
// of the next message.
type mockModel struct {
	mu     sync.Mutex
	s      script
	faults int // faults injected under s
}

// script changes what the mock model does in one scenario.
type script struct {
	parallel bool // call refund_payment and send_email in one turn
	loop     bool // never finish: call lookup_order every turn
	// failAfter names a tool: the model turn right after its successful
	// result fails with failWith ("529", "500", "400", "refusal" or
	// "max_tokens"), failTimes times, or every time when failTimes is 0.
	failAfter, failWith string
	failTimes           int
}

// useScript gives the mock model s for the rest of t, and skips t when the
// model is not the mock.
func useScript(t *testing.T, s script) {
	mockOnly(t, "it scripts the model")
	mock.set(s)
	t.Cleanup(func() { mock.set(script{}) })
}

func mockOnly(t *testing.T, why string) {
	if mode != "mock" {
		t.Skip("mock only: " + why)
	}
}

func (m *mockModel) set(s script) {
	m.mu.Lock()
	m.s, m.faults = s, 0
	m.mu.Unlock()
}

var orderID = regexp.MustCompile(`\b\d{4}\b`)

func (m *mockModel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []map[string]any `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request_error", "decoding request: "+err.Error())
		return
	}
	msgs := req.Messages
	if msg := checkHistory(msgs); msg != "" {
		apiError(w, http.StatusBadRequest, "invalid_request_error", msg)
		return
	}
	task, results := progress(msgs)

	m.mu.Lock()
	s, fault := m.s, ""
	if n := len(results); s.failAfter != "" && n > 0 && results[n-1].name == s.failAfter && !results[n-1].isErr &&
		(s.failTimes == 0 || m.faults < s.failTimes) {
		m.faults++
		fault = s.failWith
	}
	m.mu.Unlock()

	next := map[string]any{"role": "assistant", "content": []any{}}
	content := []any{map[string]any{"type": "thinking", "thinking": "", "signature": signature(msgs, next, 0)}}
	stop, details := "tool_use", any(nil)
	order := orderID.FindString(task)
	use := func(name string) {
		content = append(content, map[string]any{"type": "tool_use", "id": "toolu_" + uuid.NewString()[:8], "name": name, "input": args(name, order)})
	}
	say := func(text string) {
		content, stop = append(content, map[string]any{"type": "text", "text": text}), "end_turn"
	}
	ok, fails := map[string]bool{}, map[string]int{}
	for _, r := range results {
		if r.isErr {
			fails[r.name]++
		} else {
			ok[r.name], fails[r.name] = true, 0
		}
	}
	switch last := len(results) - 1; {
	case fault == "529" || fault == "500" || fault == "400":
		code, _ := strconv.Atoi(fault)
		kind := map[string]string{"529": "overloaded_error", "500": "api_error", "400": "invalid_request_error"}[fault]
		apiError(w, code, kind, "mock: injected "+fault)
		return
	case fault == "refusal":
		say("I can't help with that.")
		stop, details = "refusal", map[string]any{"type": "refusal", "category": "cyber", "explanation": "mock refusal"}
	case fault == "max_tokens":
		// A tool call cut off mid-input.
		content, stop = append(content, map[string]any{"type": "tool_use", "id": "toolu_" + uuid.NewString()[:8],
			"name": "send_email", "input": map[string]any{"to": "ana@example.com"}}), "max_tokens"
	case s.loop:
		use("lookup_order")
	case order == "":
		say("You're welcome.")
	case last >= 0 && results[last].isErr && fails[results[last].name] >= 2:
		say(results[last].name + " failed twice; a person will follow up.")
	case last >= 0 && results[last].isErr:
		use(results[last].name)
	case !ok["lookup_order"]:
		use("lookup_order")
	case s.parallel && !ok["refund_payment"] && !ok["send_email"]:
		use("refund_payment")
		use("send_email")
	case !ok["refund_payment"]:
		use("refund_payment")
	case !ok["send_email"]:
		use("send_email")
	default:
		say("Refunded $42.00 and emailed the customer.")
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": "msg_" + uuid.NewString()[:8], "type": "message", "role": "assistant", "model": "mock",
		"content": content, "stop_reason": stop, "stop_details": details,
		"usage": map[string]any{"input_tokens": 100, "output_tokens": 20},
	})
}

func args(tool, order string) map[string]any {
	switch tool {
	case "lookup_order":
		return map[string]any{"order_id": order}
	case "refund_payment":
		return map[string]any{"order_id": order, "amount_cents": 4200}
	}
	return map[string]any{"to": "ana@example.com", "subject": "Refund for order " + order, "body": "We refunded $42.00."}
}

type result struct {
	name  string
	isErr bool
}

// progress returns the latest user text and the tool results after it.
func progress(msgs []map[string]any) (task string, results []result) {
	names := map[string]string{} // tool_use id -> tool name
	for _, m := range msgs {
		for _, block := range blocks(m) {
			switch block["type"] {
			case "text":
				if m["role"] == "user" {
					task, results = fmt.Sprint(block["text"]), nil
				}
			case "tool_use":
				names[fmt.Sprint(block["id"])] = fmt.Sprint(block["name"])
			case "tool_result":
				results = append(results, result{names[fmt.Sprint(block["tool_use_id"])], block["is_error"] == true})
			}
		}
	}
	return task, results
}

// checkHistory runs the real API's checks on thinking blocks and tool
// results, and returns its error message, or "".
func checkHistory(msgs []map[string]any) string {
	var uses []string // the previous message's tool_use ids
	for i, m := range msgs {
		bs := blocks(m)
		var ids, answered []string
		for j, block := range bs {
			switch block["type"] {
			case "thinking":
				if block["signature"] != signature(msgs[:i], m, j) {
					return fmt.Sprintf("messages.%d.content.%d: Invalid `signature` in `thinking` block. The block is bound to a different conversation.", i, j)
				}
				if m["role"] == "assistant" && j == len(bs)-1 && i == len(msgs)-1 {
					return "the final assistant message must not end with a thinking block"
				}
			case "tool_use":
				ids = append(ids, fmt.Sprint(block["id"]))
			case "tool_result":
				id := fmt.Sprint(block["tool_use_id"])
				if !contains(uses, id) || len(answered) != j {
					return fmt.Sprintf("messages.%d.content.%d: unexpected `tool_use_id` found in `tool_result` blocks: %s. Each `tool_result` block must have a corresponding `tool_use` block in the previous message.", i, j, id)
				}
				answered = append(answered, id)
			}
		}
		if len(answered) < len(uses) {
			return fmt.Sprintf("messages.%d: `tool_use` ids were found without `tool_result` blocks immediately after: %s. Each `tool_use` block must have a corresponding `tool_result` block in the next message.", i-1, strings.Join(uses, ", "))
		}
		uses = ids
	}
	if len(uses) > 0 {
		return fmt.Sprintf("messages.%d: `tool_use` ids were found without `tool_result` blocks immediately after: %s.", len(msgs)-1, strings.Join(uses, ", "))
	}
	return ""
}

func blocks(m map[string]any) []map[string]any {
	raw, _ := m["content"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, b := range raw {
		block, _ := b.(map[string]any)
		out = append(out, block)
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
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

func apiError(w http.ResponseWriter, code int, kind, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"type": kind, "message": msg}})
}

// haikuPrefix is the only model family the real API may be called with.
const haikuPrefix = "claude-haiku-"

// meter sits between Conductor and the model endpoint. It caps the number
// of requests, stops everything on 401, 403 or 429, adds up the tokens each
// response reports and, with haikuOnly, refuses any request for a model that
// is not Haiku.
type meter struct {
	proxy       *httputil.ReverseProxy
	maxRequests int
	haikuOnly   bool

	mu                     sync.Mutex
	requests, errors       int
	times                  []time.Time // when each request was sent on
	inputTokens, outTokens int64
	cacheRead, cacheWrite  int64
	abort                  string
}

func newMeter(upstream string, maxRequests int, haikuOnly bool) *meter {
	u, err := url.Parse(upstream)
	if err != nil {
		panic(err)
	}
	m := &meter{maxRequests: maxRequests, haikuOnly: haikuOnly}
	m.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(u)
			r.Out.Host = u.Host
			// Uncompressed, so observe can read the usage block.
			r.Out.Header.Del("Accept-Encoding")
		},
		ModifyResponse: m.observe,
	}
	return m
}

func (m *meter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request_error", "e2e meter: reading request: "+err.Error())
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	var req struct {
		Model string `json:"model"`
	}
	json.Unmarshal(body, &req)

	m.mu.Lock()
	refuse := m.abort
	if refuse == "" && m.haikuOnly && !strings.HasPrefix(req.Model, haikuPrefix) {
		refuse = fmt.Sprintf("model %q refused: the real API may only be called with %s* models", req.Model, haikuPrefix)
		m.abort = refuse
	}
	if refuse == "" && m.requests >= m.maxRequests {
		refuse = fmt.Sprintf("request budget of %d spent", m.maxRequests)
		m.abort = refuse
	}
	if refuse == "" {
		m.requests++
		m.times = append(m.times, time.Now())
	}
	m.mu.Unlock()
	if refuse != "" {
		// 400, so the SDK does not retry it.
		apiError(w, http.StatusBadRequest, "invalid_request_error", "e2e meter: "+refuse)
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

// count is the number of requests sent on so far.
func (m *meter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests
}

// firstAfter is when the first request after t was sent on, or zero.
func (m *meter) firstAfter(t time.Time) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, at := range m.times {
		if at.After(t) {
			return at
		}
	}
	return time.Time{}
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
