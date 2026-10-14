package service

import (
	"encoding/json"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
)

// Gemini (including Google's OpenAI-compatible endpoint and relays in front of
// it) rejects a function parameter schema node that has no "type", which is
// exactly what JSON Schema tooling emits for a nullable field:
//
//	{"anyOf":[{"type":"string"},{"type":"null"}],"description":"..."}
//
// Codex Desktop's built-in tools (e.g. move_project_to_sidebar_section) use
// this shape, so a Responses request relayed to a Gemini model through the Chat
// Completions fallback fails with "schema didn't specify the schema type
// field". Gemini accepts the same field written as {"type":"string",
// "nullable":true}, so typeless anyOf/oneOf unions are collapsed to that form.

const geminiToolSchemaMaxDepth = 64

func isGeminiUpstreamModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if i := strings.LastIndexByte(model, '/'); i >= 0 {
		model = model[i+1:]
	}
	return strings.HasPrefix(model, "gemini")
}

// normalizeGeminiChatToolSchemas rewrites function tool parameters in place
// when the upstream model is a Gemini model. Parameters that fail to parse are
// left untouched.
func normalizeGeminiChatToolSchemas(req *apicompat.ChatCompletionsRequest, upstreamModel string) {
	if req == nil || !isGeminiUpstreamModel(upstreamModel) {
		return
	}
	for i := range req.Tools {
		fn := req.Tools[i].Function
		if fn == nil || len(fn.Parameters) == 0 {
			continue
		}
		var schema any
		if err := json.Unmarshal(fn.Parameters, &schema); err != nil {
			continue
		}
		if !normalizeGeminiSchemaNode(schema, 0) {
			continue
		}
		if out, err := json.Marshal(schema); err == nil {
			fn.Parameters = out
		}
	}
}

func normalizeGeminiSchemaNode(node any, depth int) bool {
	m, ok := node.(map[string]any)
	if !ok || depth > geminiToolSchemaMaxDepth {
		return false
	}
	changed := false
	_, hasType := m["type"]
	for _, key := range []string{"anyOf", "oneOf"} {
		// A {"type":"null"} branch is rejected even when the node has a type.
		if (!hasType || geminiUnionHasNull(m[key])) && collapseGeminiNullableUnion(m, key) {
			changed = true
			break
		}
	}
	if props, ok := m["properties"].(map[string]any); ok {
		for _, v := range props {
			if normalizeGeminiSchemaNode(v, depth+1) {
				changed = true
			}
		}
	}
	if normalizeGeminiSchemaNode(m["items"], depth+1) {
		changed = true
	}
	if normalizeGeminiSchemaNode(m["additionalProperties"], depth+1) {
		changed = true
	}
	for _, key := range []string{"anyOf", "oneOf", "allOf"} {
		if branches, ok := m[key].([]any); ok {
			for _, b := range branches {
				if normalizeGeminiSchemaNode(b, depth+1) {
					changed = true
				}
			}
		}
	}
	return changed
}

func geminiUnionHasNull(v any) bool {
	branches, _ := v.([]any)
	for _, b := range branches {
		if bm, ok := b.(map[string]any); ok {
			if t, _ := bm["type"].(string); t == "null" {
				return true
			}
		}
	}
	return false
}

// collapseGeminiNullableUnion turns {key:[X,{"type":"null"}]} into X with
// nullable:true. Keys already on the node (description, default, ...) win over
// the branch's. With several non-null branches the first one is kept, since
// Gemini cannot express the union without a type anyway.
func collapseGeminiNullableUnion(m map[string]any, key string) bool {
	branches, ok := m[key].([]any)
	if !ok || len(branches) == 0 {
		return false
	}
	var keep map[string]any
	nullable := false
	for _, b := range branches {
		bm, ok := b.(map[string]any)
		if !ok {
			continue
		}
		if t, _ := bm["type"].(string); t == "null" {
			nullable = true
			continue
		}
		if keep == nil {
			keep = bm
		}
	}
	if keep == nil {
		return false
	}
	delete(m, key)
	for k, v := range keep {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	if nullable {
		m["nullable"] = true
	}
	return true
}
