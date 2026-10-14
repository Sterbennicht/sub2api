package service

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func countCacheControl(body []byte) int {
	n := 0
	for _, key := range []string{"system", "tools"} {
		gjson.GetBytes(body, key).ForEach(func(_, v gjson.Result) bool {
			if v.Get("cache_control").Exists() {
				n++
			}
			return true
		})
	}
	gjson.GetBytes(body, "messages").ForEach(func(_, m gjson.Result) bool {
		m.Get("content").ForEach(func(_, v gjson.Result) bool {
			if v.Get("cache_control").Exists() {
				n++
			}
			return true
		})
		return true
	})
	return n
}

func TestAddConvertedRequestCacheBreakpoints_SystemStringAndMessages(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-5-5","system":"You are Codex.","tools":[{"name":"shell","input_schema":{"type":"object"}}],"messages":[
		{"role":"user","content":"hi"},
		{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"shell","input":{}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]},
		{"role":"assistant","content":[{"type":"text","text":"done"}]},
		{"role":"user","content":"next"}]}`)

	out := addConvertedRequestCacheBreakpoints(body)

	require.Equal(t, "You are Codex.", gjson.GetBytes(out, "system.0.text").String())
	require.Equal(t, "ephemeral", gjson.GetBytes(out, "system.0.cache_control.type").String())
	require.False(t, gjson.GetBytes(out, "tools.0.cache_control").Exists())
	require.True(t, gjson.GetBytes(out, "messages.4.content.0.cache_control").Exists())
	require.True(t, gjson.GetBytes(out, "messages.2.content.0.cache_control").Exists())
	require.Equal(t, 3, countCacheControl(out))
}

func TestAddConvertedRequestCacheBreakpoints_NoSystemUsesLastTool(t *testing.T) {
	body := []byte(`{"tools":[{"name":"a"},{"name":"b"}],"messages":[{"role":"user","content":"hi"}]}`)

	out := addConvertedRequestCacheBreakpoints(body)

	require.True(t, gjson.GetBytes(out, "tools.1.cache_control").Exists())
	require.False(t, gjson.GetBytes(out, "tools.0.cache_control").Exists())
	require.Equal(t, 2, countCacheControl(out))
}

func TestAddConvertedRequestCacheBreakpoints_LeavesExistingBreakpoints(t *testing.T) {
	body := []byte(`{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"hi"}]}`)

	require.Equal(t, string(body), string(addConvertedRequestCacheBreakpoints(body)))
}
