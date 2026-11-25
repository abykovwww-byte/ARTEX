package db

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/Autumn-27/artex/notify"
)

func TestTaskStatusNotificationsDeduplicateAndFanOut(t *testing.T) {
	d := notifyTestDB(t)
	task, err := d.CreateTask("private description", "private goal", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.DeleteTask(task.ID) })
	ch := newTestChannel(t, d, notify.KindTelegram, NotifyModeRealtime, `{"event_kinds":["task_completed","task_failed"]}`)
	for _, status := range []string{"running", "done", "done"} {
		if err := d.SetStatus(task.ID, status); err != nil {
			t.Fatal(err)
		}
	}
	var raw []byte
	var count int
	if err := d.QueryRow(`SELECT count(*), min(snapshot::text) FROM notification_events WHERE kind=$1 AND (snapshot->>'task_id')::bigint=$2`,
		notify.EventTaskCompleted, task.ID).Scan(&count, &raw); err != nil || count != 1 {
		t.Fatalf("completion events=%d err=%v", count, err)
	}
	var snap notify.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.TaskID != task.ID || strings.Contains(string(raw), "private") || !strings.Contains(snap.Summary, "done") {
		t.Fatalf("unexpected lifecycle snapshot: %+v", snap)
	}
	if _, _, err := d.FanOutPendingEvents(context.Background(), 10000); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT count(*) FROM notification_deliveries dl JOIN notification_events e ON e.id=dl.event_id
WHERE dl.channel_id=$1 AND (e.snapshot->>'task_id')::bigint=$2`, ch.ID, task.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("completion deliveries=%d err=%v", count, err)
	}
	if err := d.SetStatus(task.ID, "running"); err != nil {
		t.Fatal(err)
	}
	if err := d.SetStatus(task.ID, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT count(*) FROM notification_events WHERE kind=$1 AND (snapshot->>'task_id')::bigint=$2`, notify.EventTaskFailed, task.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("timeout events=%d err=%v", count, err)
	}
	if err := d.DeleteTask(task.ID); err != nil {
		t.Fatal(err)
	}
	if err := d.QueryRow(`SELECT count(*) FROM notification_events WHERE kind IN ('task_completed','task_failed') AND (snapshot->>'task_id')::bigint=$1`, task.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("deleted task retained events=%d err=%v", count, err)
	}
}

func TestGuardedTaskStatusEmitsOnlyWinningEvent(t *testing.T) {
	d := notifyTestDB(t)
	task, err := d.CreateTask("race", "race", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.DeleteTask(task.ID) })
	var wg sync.WaitGroup
	for _, status := range []string{"done", "timeout", "failed", "done"} {
		wg.Go(func() {
			if _, err := d.SetTerminalStatusGuarded(task.ID, status, ""); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var count int
	if err := d.QueryRow(`SELECT count(*) FROM notification_events WHERE kind IN ('task_completed','task_failed') AND (snapshot->>'task_id')::bigint=$1`, task.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("racing terminal writers produced %d events, err=%v", count, err)
	}
}

func TestTaskStatusSurvivesNotificationWriteFailure(t *testing.T) {
	d := notifyTestDB(t)
	task, err := d.CreateTask("delivery failure", "delivery failure", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.DeleteTask(task.ID) })
	if _, err := d.Exec(`ALTER TABLE notification_events ADD CONSTRAINT task_notify_test_never CHECK (false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Exec(`ALTER TABLE notification_events DROP CONSTRAINT task_notify_test_never`) })
	if err := d.SetStatus(task.ID, "done"); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetTask(task.ID)
	if err != nil || got.Status != "done" {
		t.Fatalf("notification failure prevented task completion: %+v err=%v", got, err)
	}
}
