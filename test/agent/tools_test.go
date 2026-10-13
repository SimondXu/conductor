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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// tools is the refund agent's tool backend, one instance per scenario.
// refund_payment honours Idempotency-Key; send_email does not, so a second
// email is a second email. Both count their real effects.
type tools struct {
	mu       sync.Mutex
	requests map[string]int    // per tool, every request
	refunds  map[string]string // Idempotency-Key -> response, one per real refund
	emails   int               // real emails sent

	// hangAfter names a tool that, once, does its effect and then withholds
	// the response until the caller goes away; effectDone is closed then.
	hangAfter  string
	effectDone chan struct{}
	// slow names a tool that, once, waits delay before doing anything;
	// started is closed when that request arrives.
	slow    string
	delay   time.Duration
	started chan struct{}
	// fail names a tool whose next request is answered 503, once;
	// failAfterEffect makes it do its effect first.
	fail            string
	failAfterEffect bool
}

func newTools() *tools {
	return &tools{requests: map[string]int{}, refunds: map[string]string{},
		effectDone: make(chan struct{}), started: make(chan struct{})}
}

func (t *tools) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/")
	var args struct {
		OrderID string `json:"order_id"`
	}
	json.NewDecoder(r.Body).Decode(&args)
	key := r.Header.Get("Idempotency-Key")

	t.mu.Lock()
	t.requests[name]++
	slow := t.slow == name
	if slow {
		t.slow = ""
		close(t.started)
	}
	fail := t.fail == name
	if fail {
		t.fail = ""
	}
	t.mu.Unlock()
	if slow {
		select {
		case <-time.After(t.delay):
		case <-r.Context().Done():
			return
		}
	}
	if fail && !t.failAfterEffect {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	t.mu.Lock()
	var resp string
	switch name {
	case "lookup_order":
		resp = fmt.Sprintf(`{"order_id":%q,"amount_cents":4200,"currency":"USD","email":"ana@example.com","status":"delivered_damaged"}`, args.OrderID)
	case "refund_payment":
		if prev, ok := t.refunds[key]; ok {
			resp = prev // the same refund again: no second effect
		} else {
			resp = fmt.Sprintf(`{"refund_id":"rf_%s","order_id":%q,"status":"refunded","amount_cents":4200}`, key[:8], args.OrderID)
			t.refunds[key] = resp
		}
	case "send_email":
		t.emails++
		resp = `{"status":"sent"}`
	default:
		t.mu.Unlock()
		http.Error(w, "unknown tool", http.StatusNotFound)
		return
	}
	hang := t.hangAfter == name
	if hang {
		t.hangAfter = ""
		close(t.effectDone)
	}
	t.mu.Unlock()
	if hang {
		<-r.Context().Done() // the effect happened; the caller never hears of it
		return
	}
	if fail {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable) // the effect happened anyway
		return
	}
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, resp)
}

// counts returns real refunds, real emails and requests per tool.
func (t *tools) counts() (refunds, emails int, requests map[string]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	requests = map[string]int{}
	for k, v := range t.requests {
		requests[k] = v
	}
	return len(t.refunds), t.emails, requests
}

// toolConfig is the registry.anthropic tools section for a tools at base.
func toolConfig(base string) string {
	schema := func(props map[string]any, required ...string) string {
		b, _ := json.Marshal(map[string]any{"type": "object", "properties": props, "required": required})
		return string(b)
	}
	str := map[string]any{"type": "string"}
	return `    tools:
      - name: lookup_order
        class: IDEMPOTENT
        description: Look up an order by id. Returns the amount paid and the customer's email.
        url: ` + base + `/lookup_order
        parameters: ` + schema(map[string]any{"order_id": str}, "order_id") + `
      - name: refund_payment
        class: IDEMPOTENT
        description: Refund an order. amount_cents is the amount to refund, in cents.
        url: ` + base + `/refund_payment
        parameters: ` + schema(map[string]any{"order_id": str, "amount_cents": map[string]any{"type": "integer"}}, "order_id", "amount_cents") + `
      - name: send_email
        class: UNSAFE
        description: Send an email to a customer.
        url: ` + base + `/send_email
        parameters: ` + schema(map[string]any{"to": str, "subject": str, "body": str}, "to", "subject", "body") + `
`
}
