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

// Package openaicompat is a recovery:full harness for any endpoint that speaks
// the OpenAI chat-completions wire format (docs/conductor/PRD.md §3). It
// follows the owned-loop turn protocol (docs/conductor/ARCHITECTURE.md §1.4):
// a model turn is one POST to {base_url}/chat/completions, and a tool turn is
// ownedloop's.
package openaicompat

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/ownedloop"
	"github.com/simondxu/conductor/proto"
)

const (
	// DefaultRequestTimeout bounds each model and tool request.
	DefaultRequestTimeout = 120 * time.Second
	// DefaultMaxTurns caps the model turns since the last user input.
	DefaultMaxTurns = 24
)

// Config is the registry.openai-compat configuration (docs/conductor/PRD.md
// §3.2).
type Config struct {
	BaseURL string `yaml:"base_url"`
	Model   string `yaml:"model"`
	// APIKeyEnv names the environment variable holding the key. The key
	// itself never appears in config or logs.
	APIKeyEnv      string        `yaml:"api_key_env,omitempty"`
	RequestTimeout time.Duration `yaml:"request_timeout,omitempty"`
	MaxTurns       int           `yaml:"max_turns,omitempty"`
	ToolChoice     string        `yaml:"tool_choice,omitempty"`
	// ExtraHeaders maps a header name to the environment variable holding
	// its value. Sent to the model endpoint only.
	ExtraHeaders map[string]string `yaml:"extra_headers,omitempty"`
	Tools        []Tool            `yaml:"tools,omitempty"`
}

// Tool is a tool the model may call. Its implementation is an HTTP endpoint.
type Tool = ownedloop.Tool

// Harness is the OpenAI-compatible harness.
type Harness struct {
	cfg      Config
	client   *http.Client
	loop     *ownedloop.Loop
	provider string // base_url host, the provider label of model metrics
}

var _ harness.Harness = (*Harness)(nil)

// New returns a harness for cfg, applying defaults.
func New(cfg Config) (*Harness, error) {
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("openaicompat: base_url %q is not an absolute URL", cfg.BaseURL)
	}
	if cfg.Model == "" {
		return nil, errors.New("openaicompat: model is required")
	}
	cfg.BaseURL = strings.TrimSuffix(cfg.BaseURL, "/")
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = DefaultMaxTurns
	}
	if cfg.ToolChoice == "" {
		cfg.ToolChoice = "auto"
	}
	tools := map[string]Tool{}
	for _, t := range cfg.Tools {
		tools[t.Name] = t
	}
	h := &Harness{
		cfg:      cfg,
		client:   &http.Client{Timeout: cfg.RequestTimeout},
		provider: u.Host,
	}
	h.loop = &ownedloop.Loop{Name: "openaicompat", Client: h.client, Tools: tools, MaxTurns: cfg.MaxTurns, ModelTurn: h.modelTurn}
	return h, nil
}

// Start implements harness.Harness. agent_config is not used.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return h.loop.Start(), nil
}

// Chat-completions wire types, limited to the fields this harness uses.
type (
	chatRequest struct {
		Model      string    `json:"model"`
		Messages   []message `json:"messages"`
		Tools      []toolDef `json:"tools,omitempty"`
		ToolChoice string    `json:"tool_choice,omitempty"`
	}
	message struct {
		Role       string     `json:"role"`
		Content    *string    `json:"content"`
		ToolCalls  []toolCall `json:"tool_calls,omitempty"`
		ToolCallID string     `json:"tool_call_id,omitempty"`
	}
	toolCall struct {
		ID       string       `json:"id"`
		Type     string       `json:"type"`
		Function functionCall `json:"function"`
	}
	functionCall struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	toolDef struct {
		Type     string      `json:"type"`
		Function functionDef `json:"function"`
	}
	functionDef struct {
		Name        string         `json:"name"`
		Description string         `json:"description,omitempty"`
		Parameters  map[string]any `json:"parameters,omitempty"`
	}
	chatResponse struct {
		Choices []struct {
			Message      message `json:"message"`
			FinishReason string  `json:"finish_reason"`
		} `json:"choices"`
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
)

func (h *Harness) modelTurn(ctx context.Context, history []*proto.Step) ([]*proto.Step, error) {
	msgs, err := messages(history)
	if err != nil {
		return nil, err
	}
	req := chatRequest{Model: h.cfg.Model, Messages: msgs}
	for _, t := range h.cfg.Tools {
		req.Tools = append(req.Tools, toolDef{Type: "function", Function: functionDef{
			Name: t.Name, Description: t.Description, Parameters: t.Parameters,
		}})
	}
	if len(req.Tools) > 0 {
		req.ToolChoice = h.cfg.ToolChoice
	}
	resp, err := h.complete(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Usage != nil {
		ownedloop.ModelTokens.WithLabelValues(h.provider, h.cfg.Model, "prompt").Add(float64(resp.Usage.PromptTokens))
		ownedloop.ModelTokens.WithLabelValues(h.provider, h.cfg.Model, "completion").Add(float64(resp.Usage.CompletionTokens))
	}
	if len(resp.Choices) == 0 {
		return nil, errors.New("openaicompat: response has no choices")
	}
	choice := resp.Choices[0]
	msg := choice.Message
	content := ""
	if msg.Content != nil {
		content = *msg.Content
	}
	if content == "" && len(msg.ToolCalls) == 0 {
		return nil, fmt.Errorf("openaicompat: model returned neither content nor tool calls (finish_reason %q)", choice.FinishReason)
	}

	var steps []*proto.Step
	if content != "" {
		steps = append(steps, &proto.Step{Type: &proto.Step_Content{Content: &proto.ContentStep{
			Role: "assistant", Content: []*proto.Content{ownedloop.TextContent(content)},
		}}})
	}
	for _, tc := range msg.ToolCalls {
		steps = append(steps, &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
			// The controller replaces this with Conductor's call_id.
			Id: tc.ID,
			Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{
				Name: tc.Function.Name, Arguments: ownedloop.ParseArguments(tc.Function.Arguments),
			}},
		}}})
	}
	return steps, nil
}

// complete POSTs req to the chat-completions endpoint.
func (h *Harness) complete(ctx context.Context, req chatRequest) (*chatResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("openaicompat: encoding request: %w", err)
	}
	endpoint := h.cfg.BaseURL + "/chat/completions"
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("openaicompat: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	if h.cfg.APIKeyEnv != "" {
		if key := os.Getenv(h.cfg.APIKeyEnv); key != "" {
			hreq.Header.Set("Authorization", "Bearer "+key)
		}
	}
	for name, env := range h.cfg.ExtraHeaders {
		if v := os.Getenv(env); v != "" {
			hreq.Header.Set(name, v)
		}
	}
	defer ownedloop.ObserveModelCall(h.provider, h.cfg.Model, time.Now())
	resp, err := h.client.Do(hreq)
	if err != nil {
		return nil, fmt.Errorf("openaicompat: model request: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, ownedloop.MaxBody))
	if err != nil {
		return nil, fmt.Errorf("openaicompat: reading model response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("openaicompat: %s returned %s: %s", endpoint, resp.Status, ownedloop.Truncate(string(data), 512))
	}
	var out chatResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("openaicompat: decoding model response: %w", err)
	}
	return &out, nil
}

// messages converts the history to chat messages. Thoughts are not sent back.
func messages(history []*proto.Step) ([]message, error) {
	var msgs []message
	for _, s := range history {
		switch {
		case s.GetContent() != nil:
			role := s.GetContent().GetRole()
			if role == "" {
				role = "user"
			}
			text := ownedloop.TextOf(s.GetContent().GetContent())
			msgs = append(msgs, message{Role: role, Content: &text})
		case s.GetToolCall() != nil:
			fc := s.GetToolCall().GetFunctionCall()
			args, err := ownedloop.ArgumentsJSON(fc.GetArguments())
			if err != nil {
				return nil, fmt.Errorf("openaicompat: tool %q args: %w", fc.GetName(), err)
			}
			tc := toolCall{ID: s.GetToolCall().GetId(), Type: "function", Function: functionCall{Name: fc.GetName(), Arguments: args}}
			// Calls from one model turn belong to one assistant message.
			if n := len(msgs); n > 0 && msgs[n-1].Role == "assistant" {
				msgs[n-1].ToolCalls = append(msgs[n-1].ToolCalls, tc)
			} else {
				msgs = append(msgs, message{Role: "assistant", ToolCalls: []toolCall{tc}})
			}
		case s.GetToolResult() != nil:
			content, err := ownedloop.ResultText(s.GetToolResult().GetFunctionResult().GetResult())
			if err != nil {
				return nil, fmt.Errorf("openaicompat: %w", err)
			}
			msgs = append(msgs, message{Role: "tool", ToolCallID: s.GetToolResult().GetCallId(), Content: &content})
		}
	}
	return msgs, nil
}
