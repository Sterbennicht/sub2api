package service

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/gin-gonic/gin"
)

const (
	// AccountTestModeTibo drives the knowledge-probe test: ask who Thibault
	// Sottiaux is and require the answer to name both OpenAI and Codex. A
	// downgraded or substituted upstream model usually cannot answer it.
	AccountTestModeTibo = "tibo"

	openAITiboTestPrompt      = "Who is Thibault Sottiaux on X? Answer from your own knowledge only, do not search."
	openAITiboReasoningEffort = "low"
)

var openAITiboKeywords = []string{"openai", "codex"}

// createOpenAITiboTestPayload mirrors a real Codex turn (model-specific base
// instructions, low reasoning effort) so the probe sees what users see.
func createOpenAITiboTestPayload(modelID string, isOAuth bool) map[string]any {
	payload := createOpenAITestPayload(modelID, isOAuth)
	payload["instructions"] = openai.CodexBaseInstructionsForModel(modelID)
	payload["input"] = []map[string]any{
		{
			"role": "user",
			"content": []map[string]any{
				{"type": "input_text", "text": openAITiboTestPrompt},
			},
		},
	}
	payload["reasoning"] = map[string]any{"effort": openAITiboReasoningEffort}
	return payload
}

// missingOpenAITiboKeywords returns the keywords the answer fails to mention;
// empty means the model recognized the person.
func missingOpenAITiboKeywords(answer string) []string {
	lower := strings.ToLower(answer)
	var missing []string
	for _, keyword := range openAITiboKeywords {
		if !strings.Contains(lower, keyword) {
			missing = append(missing, keyword)
		}
	}
	return missing
}

// openAIResponsesOutputText concatenates the output_text deltas of a raw
// Responses SSE stream.
func openAIResponsesOutputText(raw []byte) string {
	var text strings.Builder
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !sseDataPrefix.Match(line) {
			continue
		}
		var event struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
		}
		if err := json.Unmarshal(sseDataPrefix.ReplaceAll(line, nil), &event); err != nil {
			continue
		}
		if event.Type == "response.output_text.delta" {
			_, _ = text.WriteString(event.Delta)
		}
	}
	return text.String()
}

// processOpenAITiboStream relays the stream like a normal test, then judges
// the full answer before reporting completion.
func (s *AccountTestService) processOpenAITiboStream(c *gin.Context, body io.Reader) error {
	var raw bytes.Buffer
	previous, _ := c.Get(accountTestSuppressCompletionContextKey)
	c.Set(accountTestSuppressCompletionContextKey, true)
	err := s.processOpenAIStream(c, io.TeeReader(body, &raw))
	c.Set(accountTestSuppressCompletionContextKey, previous)
	if err != nil {
		return err
	}
	if missing := missingOpenAITiboKeywords(openAIResponsesOutputText(raw.Bytes())); len(missing) > 0 {
		return s.sendErrorAndEnd(c, "tibo check failed: the answer does not mention "+strings.Join(missing, ", "))
	}
	s.sendEvent(c, TestEvent{Type: "content", Text: "\n\n[tibo] passed: the answer mentions " + strings.Join(openAITiboKeywords, " + ")})
	s.sendEvent(c, TestEvent{Type: "test_complete", Success: true})
	return nil
}
