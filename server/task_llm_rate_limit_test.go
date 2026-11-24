package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
)

func TestIsRateLimitError(t *testing.T) {
	for _, message := range []string{
		"openai: status 429: saturated", "status=429", "HTTP 429", "HTTP/1.1 429",
		"rate_limit_exceeded", "Too Many Requests", "Concurrency Limit for this model (current limit: 6)",
	} {
		if !isRateLimitError(fmt.Errorf("wrapped: %w", errors.New(message))) {
			t.Errorf("not classified: %q", message)
		}
	}
	for _, err := range []error{nil, errors.New("status 4290"), errors.New("status 401"), errors.New("status 500"), errors.New("connection reset")} {
		if isRateLimitError(err) {
			t.Errorf("incorrect classification: %v", err)
		}
	}
}

func TestRateLimitRetryDelay(t *testing.T) {
	for attempt, want := range []time.Duration{5, 10, 20, 30, 30, 30} {
		if got := rateLimitRetryBackoff(attempt); got != want*time.Second {
			t.Errorf("attempt %d: %v want %v seconds", attempt, got, want)
		}
	}
	for _, tc := range []struct {
		text string
		want time.Duration
	}{
		{"status 429", 10},
		{"status 429 Retry-After: 7", 7},
		{"status 429 retry-after: 0", 1},
		{"status 429 RETRY-AFTER: -20", 1},
		{"status 429 Retry-After: 1000", 60},
		{"status 429 Retry-After: 999999999999999999999999999999", 60},
		{"status 429 Retry-After: Wed, 24 Nov", 10},
		{"status 429 Retry-After: invalid", 10},
	} {
		if got := rateLimitRetryDelay(errors.New(tc.text), 1); got != tc.want*time.Second {
			t.Errorf("%q: %v want %v seconds", tc.text, got, tc.want)
		}
	}
}

func zeroRateLimitBackoff(t *testing.T) {
	t.Helper()
	prev := rateLimitRetryBackoff
	rateLimitRetryBackoff = func(int) time.Duration { return 0 }
	t.Cleanup(func() { rateLimitRetryBackoff = prev })
}

func TestRateLimitAttemptsAndStreak(t *testing.T) {
	zeroRateLimitBackoff(t)
	// attempted is the total number of physical attempts expected for one
	// logical call: StreamAttempts<0 disables the layer, otherwise the
	// rate-limit floor of rateLimitAttempts applies and a larger configured
	// retry count wins.
	attempted := map[int]int{-1: 1, 0: rateLimitAttempts, 1: rateLimitAttempts, 9: 10}
	for _, stream := range []bool{false, true} {
		for _, configured := range []int{-1, 0, 1, 9} {
			t.Run(fmt.Sprintf("stream=%v/StreamAttempts=%d", stream, configured), func(t *testing.T) {
				resetLLMFailStreaksForTest()
				t.Cleanup(resetLLMFailStreaksForTest)
				// Preserve an existing streak; saturation must neither burn nor reset it.
				noteLLMCallFailure("rate-limit", 7)
				p := &scriptedLLMProvider{err: errors.New("openai: status 429: concurrency limit")}
				hooks := taskLLMStreamHooks{
					current: func(context.Context) (taskLLMSelection, error) {
						return taskLLMSelection{profileID: 7, provider: p, retry: agent.RetryConfig{StreamAttempts: configured}}, nil
					},
					exhaust: func(taskLLMSelection, error) (db.TaskLLMTransition, error) {
						t.Fatal("rate limit exhausted a healthy profile")
						return db.TaskLLMTransition{}, nil
					},
				}
				for range 3 {
					before := p.calls
					if got := runLogicalCall(context.Background(), stream, "rate-limit", hooks); !errors.Is(got.err, p.err) {
						t.Fatalf("error=%v want %v", got.err, p.err)
					}
					if got := p.calls - before; got != attempted[configured] {
						t.Fatalf("attempts=%d want %d", got, attempted[configured])
					}
				}
				llmFailStreaks.Lock()
				streak := llmFailStreaks.m[llmFailKey("rate-limit", 7)]
				llmFailStreaks.Unlock()
				if streak != 1 {
					t.Fatalf("streak=%d want original 1", streak)
				}
			})
		}
	}
}

func TestRateLimitCommittedStreamIsNotReplayed(t *testing.T) {
	zeroRateLimitBackoff(t)
	p := &scriptedLLMProvider{events: []llm.StreamEvent{{Type: llm.SETextDelta, Text: "partial"}}, err: errors.New("status 429")}
	hooks := taskLLMStreamHooks{current: func(context.Context) (taskLLMSelection, error) {
		return taskLLMSelection{profileID: 7, provider: p}, nil
	}}
	got := runLogicalCall(context.Background(), true, "rate-limit-committed", hooks)
	if !errors.Is(got.err, p.err) || p.calls != 1 || got.text != "partial" {
		t.Fatalf("calls=%d result=%+v", p.calls, got)
	}
}

func TestRateLimitBackoffCancellation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		p := &scriptedLLMProvider{err: errors.New("status 429 Retry-After: 60")}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		hooks := taskLLMStreamHooks{current: func(context.Context) (taskLLMSelection, error) {
			return taskLLMSelection{provider: p}, nil
		}}
		got := runLogicalCall(ctx, stream, "rate-limit-cancel", hooks)
		cancel()
		if !errors.Is(got.err, context.DeadlineExceeded) || p.calls != 1 {
			t.Fatalf("stream=%v calls=%d error=%v", stream, p.calls, got.err)
		}
	}
}

// Exercise the real HTTP provider adapter, not just a scripted provider: a
// saturated endpoint recovers on the fourth request without ending the call.
func TestRateLimitHTTPRecovery(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			calls := 0
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if calls < 4 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					fmt.Fprint(w, `{"error":{"message":"concurrency limit for this model (current limit: 6); Retry-After: 1","type":"rate_limit"}}`)
					return
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"}}]}\n\ndata: [DONE]\n\n")
				} else {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"recovered"},"finish_reason":"stop"}]}`)
				}
			}))
			defer endpoint.Close()
			provider, err := (agent.Config{Format: llm.FormatOpenAI, BaseURL: endpoint.URL + "/v1", Model: "test", APIKey: "test", Retry: agent.RetryConfig{ConnectAttempts: -1}}).NewProvider()
			if err != nil {
				t.Fatal(err)
			}
			hooks := taskLLMStreamHooks{current: func(context.Context) (taskLLMSelection, error) {
				// Default retry policy: the rate-limit floor applies on top of it.
				return taskLLMSelection{profileID: 7, provider: provider}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			started := time.Now()
			got := runLogicalCall(ctx, stream, "rate-limit-http", hooks)
			if got.err != nil || got.text != "recovered" || calls != 4 {
				t.Fatalf("HTTP calls=%d result=%+v", calls, got)
			}
			if elapsed := time.Since(started); elapsed < 3*time.Second {
				t.Fatalf("Retry-After ignored: three waits took %v", elapsed)
			}
		})
	}
}
