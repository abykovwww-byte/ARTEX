package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/transcript"
)

type chatRecoveryProbe struct {
	requests []llm.CompletionRequest
	next     func(int, func(llm.StreamEvent, error) bool)
}

func (p *chatRecoveryProbe) Stream(_ context.Context, req llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	p.requests = append(p.requests, req)
	call := len(p.requests)
	return func(yield func(llm.StreamEvent, error) bool) { p.next(call, yield) }
}

func (p *chatRecoveryProbe) Complete(context.Context, llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	return llm.Message{}, "", llm.Usage{}, errors.New("unexpected Complete call")
}

func TestChatRecoversEOFWithoutReplayingTools(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "sessions", "conv-eof", "saved.txt")
	input, _ := json.Marshal(map[string]string{"file_path": marker, "content": "saved once"})
	p := &chatRecoveryProbe{next: func(call int, yield func(llm.StreamEvent, error) bool) {
		var events []llm.StreamEvent
		switch call {
		case 1:
			events = []llm.StreamEvent{
				{Type: llm.SEToolUseStart, ToolID: "save", ToolName: "Write"},
				{Type: llm.SEToolInputJSON, Text: string(input)},
				{Type: llm.SEMessageDelta, StopReason: "tool_use"},
			}
		case 2:
			events = []llm.StreamEvent{
				{Type: llm.SEMessageStart, Usage: llm.Usage{InputTokens: 7}},
				{Type: llm.SETextDelta, Text: "discarded partial answer"},
				{Type: llm.SEToolUseStart, ToolID: "incomplete", ToolName: "Write"},
				{Type: llm.SEToolInputJSON, Text: `{"file_path":`},
			}
		case 3:
			events = []llm.StreamEvent{
				{Type: llm.SEMessageStart, Usage: llm.Usage{InputTokens: 11}},
				{Type: llm.SETextDelta, Text: "Recovered answer"},
				{Type: llm.SEMessageDelta, StopReason: "end_turn"},
			}
		default:
			t.Fatalf("unexpected provider call %d", call)
		}
		for _, event := range events {
			if !yield(event, nil) {
				return
			}
		}
		if call == 2 {
			yield(llm.StreamEvent{}, io.ErrUnexpectedEOF)
		}
	}}
	store := transcript.NewStore(t.TempDir())
	chat := NewChatAgent(p, "test-model", dir, store, 100000)
	var activities []db.Activity
	text, err := chat.Chat(t.Context(), "assistant", "conv-eof", "Save the file and answer", 3, time.Minute, false, func(a db.Activity) {
		activities = append(activities, a)
	})
	if err != nil || text != "Recovered answer" {
		t.Fatalf("text=%q error=%v", text, err)
	}
	if len(p.requests) != 3 || !reflect.DeepEqual(p.requests[1], p.requests[2]) {
		t.Fatal("recovery changed the request or repeated an earlier model turn")
	}
	b, err := os.ReadFile(marker)
	if err != nil || string(b) != "saved once" {
		t.Fatalf("saved tool result lost: %q, %v", b, err)
	}
	var tools, results, reconnects int
	for _, a := range activities {
		if a.IsError || strings.Contains(a.Detail, "discarded partial") {
			t.Fatalf("recovered chat exposed an error or discarded answer: %+v", a)
		}
		if a.Kind == "tool_use" {
			tools++
		}
		if a.Kind == "result" {
			results++
			if a.InputTokens == nil || *a.InputTokens != 18 {
				t.Fatalf("failed attempt usage lost: %+v", a)
			}
		}
		if strings.Contains(a.Summary, "Reconnecting, attempt 1/2") {
			reconnects++
		}
	}
	if tools != 1 || results != 1 || reconnects != 1 {
		t.Fatalf("tools=%d results=%d reconnects=%d", tools, results, reconnects)
	}
}

func TestChatEOFRecoveryIsBoundedAndPreservesPartialAnswer(t *testing.T) {
	p := &chatRecoveryProbe{next: func(_ int, yield func(llm.StreamEvent, error) bool) {
		if yield(llm.StreamEvent{Type: llm.SETextDelta, Text: "partial answer"}, nil) {
			yield(llm.StreamEvent{}, io.ErrUnexpectedEOF)
		}
	}}
	chat := NewChatAgent(p, "test-model", t.TempDir(), nil, 100000)
	var activities []db.Activity
	_, err := chat.Chat(t.Context(), "assistant", "conv-exhausted", "hello", 1, time.Minute, false, func(a db.Activity) {
		activities = append(activities, a)
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) || len(p.requests) != 3 {
		t.Fatalf("error=%v calls=%d", err, len(p.requests))
	}
	var partial, failed int
	for _, a := range activities {
		if a.Detail == "partial answer" {
			partial++
		}
		if a.Kind == "result" && a.IsError {
			failed++
			if strings.Contains(a.Detail, "marked blocked") {
				t.Fatal("chat failure claims the task is blocked")
			}
		}
	}
	if partial != 1 || failed != 1 {
		t.Fatalf("partial=%d failed=%d", partial, failed)
	}
}

func TestChatEOFRecoveryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	p := &chatRecoveryProbe{next: func(_ int, yield func(llm.StreamEvent, error) bool) {
		yield(llm.StreamEvent{}, io.ErrUnexpectedEOF)
	}}
	chat := NewChatAgent(p, "test-model", t.TempDir(), nil, 100000)
	_, err := chat.Chat(ctx, "assistant", "conv-cancelled", "hello", 1, time.Minute, false, func(a db.Activity) {
		if strings.Contains(a.Summary, "Reconnecting") {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) || len(p.requests) != 1 {
		t.Fatalf("error=%v calls=%d", err, len(p.requests))
	}
}

func TestChatDoesNotRetryUnrelatedModelErrors(t *testing.T) {
	wantErr := errors.New("status 401: invalid API key")
	p := &chatRecoveryProbe{next: func(_ int, yield func(llm.StreamEvent, error) bool) {
		yield(llm.StreamEvent{}, wantErr)
	}}
	chat := NewChatAgent(p, "test-model", t.TempDir(), nil, 100000)
	_, err := chat.Chat(t.Context(), "assistant", "conv-auth-error", "hello", 1, time.Minute, false, nil)
	if !errors.Is(err, wantErr) || len(p.requests) != 1 {
		t.Fatalf("error=%v calls=%d", err, len(p.requests))
	}
}
