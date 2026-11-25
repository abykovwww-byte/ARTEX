package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCompactionInheritsProviderThinking(t *testing.T) {
	for _, thinking := range []string{"enabled", "disabled", ""} {
		t.Run("thinking="+thinking, func(t *testing.T) {
			var request struct {
				Thinking struct {
					Type string `json:"type"`
				} `json:"thinking"`
				OutputConfig struct {
					Effort string `json:"effort"`
				} `json:"output_config"`
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"test","type":"message","role":"assistant","model":"test-model","content":[{"type":"text","text":"condensed"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}`)
			}))
			defer upstream.Close()
			cfg := ConfigFrom("anthropic", "test-model", upstream.URL, "test-key", "")
			cfg.ThinkingType = thinking
			cfg.ReasoningEffort = "medium"
			provider, err := cfg.NewProvider()
			if err != nil {
				t.Fatal(err)
			}
			compactor := NewCompactor(provider, "test-model")
			body, err := compactor.compress(context.Background(), newColdGraph(nil, nil), block{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if body != "condensed" || request.Thinking.Type != thinking || request.OutputConfig.Effort != "medium" {
				t.Fatalf("body=%q thinking=%q effort=%q, want condensed/%q/medium", body, request.Thinking.Type, request.OutputConfig.Effort, thinking)
			}
		})
	}
}
