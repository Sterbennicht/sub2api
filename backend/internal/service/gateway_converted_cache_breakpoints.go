package service

import (
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// addConvertedRequestCacheBreakpoints gives an Anthropic body converted from an
// OpenAI Responses request the prompt-cache breakpoints a native Claude client
// would send. The converter emits no cache_control at all, so Codex sessions
// on Claude re-bill the whole ~50K-token prefix (instructions + tool schemas)
// at full input price every turn.
//
// Breakpoints: the last system block (covers tools + system; falls back to the
// last tool when there is no system), plus addMessageCacheBreakpoints' two
// message breakpoints — at most 3 of Anthropic's 4. Bodies that already carry
// any cache_control are left alone.
func addConvertedRequestCacheBreakpoints(body []byte) []byte {
	if len(body) == 0 || hasAnyCacheControl(body) {
		return body
	}
	cc := fmt.Sprintf(`{"type":"ephemeral","ttl":%q}`, claude.DefaultCacheControlTTL)

	system := gjson.GetBytes(body, "system")
	switch {
	case system.Type == gjson.String && system.String() != "":
		block := fmt.Sprintf(`[{"type":"text","text":%s,"cache_control":%s}]`, mustJSONString(system.String()), cc)
		if next, err := sjson.SetRawBytes(body, "system", []byte(block)); err == nil {
			body = next
		}
	case system.IsArray() && len(system.Array()) > 0:
		path := fmt.Sprintf("system.%d.cache_control", len(system.Array())-1)
		if next, err := sjson.SetRawBytes(body, path, []byte(cc)); err == nil {
			body = next
		}
	default:
		if tools := gjson.GetBytes(body, "tools"); tools.IsArray() && len(tools.Array()) > 0 {
			path := fmt.Sprintf("tools.%d.cache_control", len(tools.Array())-1)
			if next, err := sjson.SetRawBytes(body, path, []byte(cc)); err == nil {
				body = next
			}
		}
	}
	return addMessageCacheBreakpoints(body)
}

func hasAnyCacheControl(body []byte) bool {
	found := false
	check := func(arr gjson.Result) {
		arr.ForEach(func(_, item gjson.Result) bool {
			if item.Get("cache_control").Exists() {
				found = true
			}
			return !found
		})
	}
	check(gjson.GetBytes(body, "system"))
	check(gjson.GetBytes(body, "tools"))
	gjson.GetBytes(body, "messages").ForEach(func(_, msg gjson.Result) bool {
		check(msg.Get("content"))
		return !found
	})
	return found
}
