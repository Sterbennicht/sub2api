//go:build unit

package service

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func tiboProbeSSEBody(answer string) string {
	half := len(answer) / 2
	return "data: {\"type\":\"response.output_text.delta\",\"delta\":" + mustJSONString(answer[:half]) + "}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":" + mustJSONString(answer[half:]) + "}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_tibo\"}}\n\n"
}

func runTiboProbe(t *testing.T, answer string) (error, string, *httpUpstreamRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	ctx, recorder := newTestContext()
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(tiboProbeSSEBody(answer))),
	}}
	svc := &AccountTestService{
		httpUpstream: upstream,
		cfg:          &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false}}},
	}
	account := &Account{
		ID:          96,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":  "sk-test",
			"base_url": "https://compat-upstream.example/v1",
		},
		Extra: map[string]any{openai_compat.ExtraKeyResponsesSupported: true},
	}
	err := svc.testOpenAIAccountConnection(ctx, account, "gpt-6.1-sol", "", AccountTestModeTibo)
	return err, recorder.Body.String(), upstream
}

func TestAccountTestService_OpenAITiboPassesWhenAnswerNamesOpenAIAndCodex(t *testing.T) {
	err, events, upstream := runTiboProbe(t, "Thibault Sottiaux is an engineering leader at OpenAI working on Codex.")
	require.NoError(t, err)

	require.Equal(t, "https://compat-upstream.example/v1/responses", upstream.lastReq.URL.String())
	require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, openAITiboTestPrompt, gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String())
	require.Equal(t, "low", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	require.Equal(t, openai.CodexBaseInstructionsForModel("gpt-6.1-sol"), gjson.GetBytes(upstream.lastBody, "instructions").String())

	require.Contains(t, events, "[tibo] passed")
	require.Equal(t, 1, strings.Count(events, `"type":"test_complete"`))
	require.Contains(t, events, `"success":true`)
}

func TestAccountTestService_OpenAITiboFailsWhenAnswerDoesNotKnowThePerson(t *testing.T) {
	err, events, _ := runTiboProbe(t, "I don't have reliable information about that person. They may work at OpenAI.")
	require.ErrorContains(t, err, "does not mention codex")

	require.Contains(t, events, `"type":"error"`)
	require.NotContains(t, events, `"type":"test_complete"`)
}

func TestMissingOpenAITiboKeywords(t *testing.T) {
	require.Empty(t, missingOpenAITiboKeywords("He leads CODEX at OpenAI."))
	require.Equal(t, []string{"openai", "codex"}, missingOpenAITiboKeywords("No idea."))
	require.Equal(t, []string{"openai", "codex"}, missingOpenAITiboKeywords(""))
}

func TestAccountTestService_OpenAITiboRejectsChatCompletionsOnlyUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := newTestContext()
	upstream := &httpUpstreamRecorder{}
	svc := &AccountTestService{
		httpUpstream: upstream,
		cfg:          &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false}}},
	}
	account := &Account{
		ID:          97,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Concurrency: 1,
		Credentials: map[string]any{
			"api_key":  "sk-test",
			"base_url": "https://compat-upstream.example/v1",
		},
		Extra: map[string]any{openai_compat.ExtraKeyResponsesSupported: false},
	}

	err := svc.testOpenAIAccountConnection(ctx, account, "gpt-6.1-sol", "", AccountTestModeTibo)
	require.ErrorContains(t, err, "Responses API")
	require.Nil(t, upstream.lastReq)
}
