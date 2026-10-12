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

// Package anthropic is a recovery:full harness for the Anthropic Messages API
// (docs/conductor/PRD.md §3.5). It follows the owned-loop turn protocol
// (docs/conductor/ARCHITECTURE.md §1.4): a model turn is one Messages request,
// and a tool turn is ownedloop's.
//
// The history it sends is rebuilt from the event log on every turn, so it is
// append-only by construction: each logged step maps to the same content block
// every time. Thinking blocks are logged as thought steps with their signature
// and sent back unchanged, which is what keeps them valid across tool turns.
package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"github.com/simondxu/conductor/internal/harness"
	"github.com/simondxu/conductor/internal/harness/ownedloop"
	"github.com/simondxu/conductor/proto"
)

const (
	DefaultBaseURL   = "https://api.anthropic.com"
	DefaultAPIKeyEnv = "ANTHROPIC_API_KEY"
	// DefaultMaxTokens caps one response.
	DefaultMaxTokens = 16000
	// DefaultRequestTimeout bounds each model and tool request.
	DefaultRequestTimeout = 120 * time.Second
	// DefaultMaxTurns caps the model turns since the last user input.
	DefaultMaxTurns = 24
)

// Config is the registry.anthropic configuration (docs/conductor/PRD.md §3.5).
type Config struct {
	Model string `yaml:"model"`
	// BaseURL defaults to DefaultBaseURL.
	BaseURL string `yaml:"base_url,omitempty"`
	// APIKeyEnv names the environment variable holding the key, by default
	// ANTHROPIC_API_KEY. The key itself never appears in config or logs.
	APIKeyEnv string `yaml:"api_key_env,omitempty"`
	MaxTokens int64  `yaml:"max_tokens,omitempty"`
	// Effort is output_config.effort. Empty leaves the model's default.
	Effort         string        `yaml:"effort,omitempty"`
	RequestTimeout time.Duration `yaml:"request_timeout,omitempty"`
	MaxTurns       int           `yaml:"max_turns,omitempty"`
	Tools          []Tool        `yaml:"tools,omitempty"`
}

// Tool is a tool the model may call. Its implementation is an HTTP endpoint.
type Tool = ownedloop.Tool

// Harness is the Anthropic harness.
type Harness struct {
	cfg      Config
	client   sdk.Client
	tools    []sdk.ToolUnionParam
	loop     *ownedloop.Loop
	provider string // base_url host, the provider label of model metrics
}

var _ harness.Harness = (*Harness)(nil)

// New returns a harness for cfg, applying defaults.
func New(cfg Config) (*Harness, error) {
	if cfg.Model == "" {
		return nil, errors.New("anthropic: model is required")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("anthropic: base_url %q is not an absolute URL", cfg.BaseURL)
	}
	if cfg.APIKeyEnv == "" {
		cfg.APIKeyEnv = DefaultAPIKeyEnv
	}
	if cfg.MaxTokens == 0 {
		cfg.MaxTokens = DefaultMaxTokens
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxTurns == 0 {
		cfg.MaxTurns = DefaultMaxTurns
	}
	h := &Harness{
		cfg: cfg,
		// Credentials and the endpoint come from this config only, never
		// from the SDK's environment or profile autoload. The SDK keeps its
		// default retries: a model request has no side effects, and 429 and
		// 529 are routine.
		client: sdk.NewClient(
			option.WithoutEnvironmentDefaults(),
			option.WithBaseURL(strings.TrimSuffix(cfg.BaseURL, "/")+"/"),
			option.WithRequestTimeout(cfg.RequestTimeout),
		),
		provider: u.Host,
	}
	tools := map[string]Tool{}
	for _, t := range cfg.Tools {
		tools[t.Name] = t
		h.tools = append(h.tools, sdk.ToolUnionParam{OfTool: toolParam(t)})
	}
	h.loop = &ownedloop.Loop{
		Name: "anthropic", Client: &http.Client{Timeout: cfg.RequestTimeout},
		Tools: tools, MaxTurns: cfg.MaxTurns, ModelTurn: h.modelTurn,
	}
	return h, nil
}

// toolParam maps a tool's JSON schema onto the SDK's input schema, whose
// type is always object.
func toolParam(t Tool) *sdk.ToolParam {
	schema := sdk.ToolInputSchemaParam{ExtraFields: map[string]any{}}
	for k, v := range t.Parameters {
		if k != "type" {
			schema.ExtraFields[k] = v
		}
	}
	p := &sdk.ToolParam{Name: t.Name, InputSchema: schema}
	if t.Description != "" {
		p.Description = sdk.String(t.Description)
	}
	return p
}

// Start implements harness.Harness. agent_config is not used.
func (h *Harness) Start(ctx context.Context, conversationID string, config []byte) (harness.Execution, error) {
	return h.loop.Start(), nil
}

func (h *Harness) modelTurn(ctx context.Context, history []*proto.Step) ([]*proto.Step, error) {
	msgs, err := messages(history)
	if err != nil {
		return nil, err
	}
	params := sdk.MessageNewParams{
		Model:     sdk.Model(h.cfg.Model),
		MaxTokens: h.cfg.MaxTokens,
		Messages:  msgs,
		Tools:     h.tools,
		// The history only grows, so each turn can read the previous turn's
		// cache. The server places the breakpoint on the last block.
		CacheControl: sdk.NewCacheControlEphemeralParam(),
	}
	if h.cfg.Effort != "" {
		params.OutputConfig = sdk.OutputConfigParam{Effort: sdk.OutputConfigEffort(h.cfg.Effort)}
	}
	var opts []option.RequestOption
	if key := os.Getenv(h.cfg.APIKeyEnv); key != "" {
		opts = append(opts, option.WithAPIKey(key))
	}

	start := time.Now()
	resp, err := h.client.Messages.New(ctx, params, opts...)
	ownedloop.ObserveModelCall(h.provider, h.cfg.Model, start)
	if err != nil {
		return nil, fmt.Errorf("anthropic: model request: %w", err)
	}
	if resp.JSON.Usage.Valid() {
		u := resp.Usage
		ownedloop.ModelTokens.WithLabelValues(h.provider, h.cfg.Model, "prompt").Add(float64(u.InputTokens + u.CacheReadInputTokens + u.CacheCreationInputTokens))
		ownedloop.ModelTokens.WithLabelValues(h.provider, h.cfg.Model, "completion").Add(float64(u.OutputTokens))
	}
	switch resp.StopReason {
	case sdk.StopReasonEndTurn, sdk.StopReasonToolUse, sdk.StopReasonStopSequence:
	case sdk.StopReasonRefusal:
		return nil, fmt.Errorf("anthropic: model refused (category %q): %s", resp.StopDetails.Category, resp.StopDetails.Explanation)
	default:
		// max_tokens leaves a truncated answer or tool input; logging it as
		// the model's answer would be wrong.
		return nil, fmt.Errorf("anthropic: response stopped with %q", resp.StopReason)
	}

	var steps []*proto.Step
	answered := false
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case sdk.ThinkingBlock:
			th := &proto.ThoughtStep{Signature: []byte(b.Signature)}
			if b.Thinking != "" {
				th.Summary = []*proto.Content{ownedloop.TextContent(b.Thinking)}
			}
			steps = append(steps, &proto.Step{Type: &proto.Step_Thought{Thought: th}})
		case sdk.TextBlock:
			if b.Text == "" {
				continue // the API rejects an empty text block sent back
			}
			steps = append(steps, &proto.Step{Type: &proto.Step_Content{Content: &proto.ContentStep{
				Role: "assistant", Content: []*proto.Content{ownedloop.TextContent(b.Text)},
			}}})
			answered = true
		case sdk.ToolUseBlock:
			steps = append(steps, &proto.Step{Type: &proto.Step_ToolCall{ToolCall: &proto.ToolCallStep{
				// The controller replaces this with Conductor's call_id
				// before the call is logged or sent back.
				Id: b.ID,
				Type: &proto.ToolCallStep_FunctionCall{FunctionCall: &proto.FunctionCallStep{
					Name: b.Name, Arguments: ownedloop.ParseArguments(string(b.Input)),
				}},
			}}})
			answered = true
		default:
			// A block this harness cannot log could not be sent back, and
			// dropping it would invalidate every later thinking block.
			return nil, fmt.Errorf("anthropic: unsupported content block %q", block.Type)
		}
	}
	if !answered {
		return nil, fmt.Errorf("anthropic: model returned neither text nor tool calls (stop_reason %q)", resp.StopReason)
	}
	return steps, nil
}

// messages converts the history to Messages API messages. Consecutive steps
// of one role form one message, and each step is one content block in log
// order, so a model turn goes back exactly as it came.
func messages(history []*proto.Step) ([]sdk.MessageParam, error) {
	var msgs []sdk.MessageParam
	add := func(role sdk.MessageParamRole, block sdk.ContentBlockParamUnion) {
		if n := len(msgs); n > 0 && msgs[n-1].Role == role {
			msgs[n-1].Content = append(msgs[n-1].Content, block)
			return
		}
		msgs = append(msgs, sdk.MessageParam{Role: role, Content: []sdk.ContentBlockParamUnion{block}})
	}
	for _, s := range history {
		switch {
		case s.GetContent() != nil:
			role := sdk.MessageParamRoleUser
			if s.GetContent().GetRole() == "assistant" {
				role = sdk.MessageParamRoleAssistant
			}
			add(role, sdk.NewTextBlock(ownedloop.TextOf(s.GetContent().GetContent())))
		case s.GetThought() != nil:
			th := s.GetThought()
			add(sdk.MessageParamRoleAssistant, sdk.NewThinkingBlock(string(th.GetSignature()), ownedloop.TextOf(th.GetSummary())))
		case s.GetToolCall() != nil:
			fc := s.GetToolCall().GetFunctionCall()
			args, err := ownedloop.ArgumentsJSON(fc.GetArguments())
			if err != nil {
				return nil, fmt.Errorf("anthropic: tool %q args: %w", fc.GetName(), err)
			}
			add(sdk.MessageParamRoleAssistant, sdk.NewToolUseBlock(s.GetToolCall().GetId(), json.RawMessage(args), fc.GetName()))
		case s.GetToolResult() != nil:
			r := s.GetToolResult().GetFunctionResult()
			content, err := ownedloop.ResultText(r.GetResult())
			if err != nil {
				return nil, fmt.Errorf("anthropic: %w", err)
			}
			add(sdk.MessageParamRoleUser, sdk.NewToolResultBlock(s.GetToolResult().GetCallId(), content, r.GetIsError()))
		}
	}
	return msgs, nil
}
