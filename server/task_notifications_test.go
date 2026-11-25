package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/notify"
)

func TestTaskLifecycleDeliversOncePerTransitionThroughTelegramChannel(t *testing.T) {
	f := newNotifyFixture(t)
	messages := make(chan string, 4)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(400)
			return
		}
		messages <- payload.Text
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"ok":true,"result":{"message_id":123}}`)
	}))
	defer receiver.Close()
	body, _ := json.Marshal(map[string]any{
		"name": "task status", "kind": "telegram", "mode": "realtime", "enabled": true,
		"config": map[string]string{"bot_token": "test-token", "chat_id": "123", "base_url": receiver.URL},
		"filter": map[string]any{"event_kinds": []string{notify.EventTaskCompleted, notify.EventTaskFailed}},
	})
	r := f.request("POST", "/api/notify/channels", string(body))
	if r.Code != 200 && r.Code != 201 {
		t.Fatal(r.Code, r.Body)
	}
	var created struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &created); err != nil || created.ID == 0 {
		t.Fatalf("channel not created: %s err=%v", r.Body, err)
	}
	t.Cleanup(func() { f.pg.DeleteNotificationChannel(context.Background(), created.ID) })
	ch, err := f.pg.NotificationChannelByID(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"done", "done", "running", "failed", "failed"} {
		if err := f.pg.SetStatus(f.taskID, status); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := f.pg.FanOutPendingEvents(context.Background(), 1000); err != nil {
		t.Fatal(err)
	}
	f.n.stepRealtime(context.Background(), ch, 10, "https://artex.local")
	f.n.stepRealtime(context.Background(), ch, 10, "https://artex.local")
	if len(messages) != 2 {
		t.Fatalf("sent %d messages for two transitions", len(messages))
	}
	for range 2 {
		text := <-messages
		if !strings.Contains(text, fmt.Sprintf("#%d", f.taskID)) || !strings.Contains(text, fmt.Sprintf("/function/tasks/detail?id=%d", f.taskID)) || strings.Contains(text, "/function/findings/") {
			t.Fatalf("wrong task notification: %s", text)
		}
	}
	var sent int
	if err := f.pg.QueryRow(`SELECT count(*) FROM notification_deliveries WHERE channel_id=$1 AND state='sent'`, ch.ID).Scan(&sent); err != nil || sent != 2 {
		t.Fatalf("sent delivery count=%d err=%v", sent, err)
	}
}
