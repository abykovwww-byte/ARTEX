package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
)

type telegramReceiptTransport struct {
	base http.RoundTripper
	id   int64
}

func (p *telegramReceiptTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := p.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	var receipt struct {
		Result struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &receipt) == nil {
		p.id = receipt.Result.MessageID
	}
	return resp, nil
}

// Explicit opt-in only: normal tests and CI never send Telegram messages.
func TestLiveTelegramTaskNotification(t *testing.T) {
	token, chat := os.Getenv("ARTEX_LIVE_TELEGRAM_BOT_TOKEN"), os.Getenv("ARTEX_LIVE_TELEGRAM_CHAT_ID")
	if token == "" || chat == "" {
		t.Skip("live Telegram acceptance requires explicit credentials")
	}
	previous := httpClient
	probe := &telegramReceiptTransport{base: previous.Transport}
	client := *previous
	client.Transport = probe
	httpClient = &client
	t.Cleanup(func() { httpClient = previous })
	kept, err := (telegramChannel{}).Send(context.Background(), map[string]any{"bot_token": token, "chat_id": chat}, Message{
		Items: []Item{{Kind: EventTaskCompleted, Name: "🧪 ARTEX: проверка уведомлений", Summary: "Тест нового канала: завершение кейса и ошибка/таймаут. Это проверка доставки, статус рабочих кейсов не менялся.", DetailURL: "http://192.168.1.88:8787/system/notify"}},
	})
	if err != nil || kept != 1 || probe.id <= 0 {
		t.Fatalf("Telegram acceptance failed: kept=%d message_id=%d err=%v", kept, probe.id, err)
	}
	t.Logf("Telegram accepted message_id=%d", probe.id)
}
