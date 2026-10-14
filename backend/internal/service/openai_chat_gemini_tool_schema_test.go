package service

import (
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/stretchr/testify/require"
)

func geminiSchemaTestRequest(params string) *apicompat.ChatCompletionsRequest {
	return &apicompat.ChatCompletionsRequest{Tools: []apicompat.ChatTool{{
		Type:     "function",
		Function: &apicompat.ChatFunction{Name: "move_project", Parameters: json.RawMessage(params)},
	}}}
}

func TestNormalizeGeminiChatToolSchemas_CollapsesNullableUnion(t *testing.T) {
	req := geminiSchemaTestRequest(`{"type":"object","properties":{"sectionId":{"anyOf":[{"type":"string","minLength":1},{"type":"null"}],"description":"dest"},"tags":{"type":"array","items":{"oneOf":[{"type":"integer"},{"type":"null"}]}}},"required":["sectionId"]}`)

	normalizeGeminiChatToolSchemas(req, "gemini-4-argon")

	var got map[string]any
	require.NoError(t, json.Unmarshal(req.Tools[0].Function.Parameters, &got))
	props := got["properties"].(map[string]any)
	require.Equal(t, map[string]any{"type": "string", "minLength": float64(1), "description": "dest", "nullable": true}, props["sectionId"])
	require.Equal(t, map[string]any{"type": "integer", "nullable": true}, props["tags"].(map[string]any)["items"])
}

func TestNormalizeGeminiChatToolSchemas_OnlyForGeminiModels(t *testing.T) {
	params := `{"type":"object","properties":{"a":{"anyOf":[{"type":"string"},{"type":"null"}]}}}`
	req := geminiSchemaTestRequest(params)

	normalizeGeminiChatToolSchemas(req, "gpt-6.1-sol")

	require.JSONEq(t, params, string(req.Tools[0].Function.Parameters))
}

func TestNormalizeGeminiChatToolSchemas_TypedNodesWithProviderPrefix(t *testing.T) {
	req := geminiSchemaTestRequest(`{"type":"object","properties":{"a":{"type":"string","anyOf":[{"type":"string"},{"type":"null"}]},"b":{"type":"string","anyOf":[{"enum":["x"]},{"enum":["y"]}]}}}`)

	normalizeGeminiChatToolSchemas(req, "google/gemini-4-argon")

	require.JSONEq(t, `{"type":"object","properties":{"a":{"type":"string","nullable":true},"b":{"type":"string","anyOf":[{"enum":["x"]},{"enum":["y"]}]}}}`, string(req.Tools[0].Function.Parameters))
}
