package tests

import (
	"testing"
	"time"

	"github.com/AbhiAnand-1011/FluxGuard/internal"
)

func windowEvent(id, key string, timestamp time.Time) internal.Event {
	return internal.Event{
		ID:        id,
		Type:      "transaction",
		Source:    "payment-service",
		Key:       key,
		Value:     100,
		Timestamp: timestamp,
	}
}

func TestWindowRemovesExpiredEvents(t *testing.T) {
	window := internal.NewWindow(10 * time.Second)

	base := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

	window.Add(windowEvent("evt-1", "user-42", base))
	window.Add(windowEvent("evt-2", "user-42", base.Add(5*time.Second)))

	events := window.Get("user-42")

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	events = window.Add(
		windowEvent("evt-3", "user-42", base.Add(11*time.Second)),
	)

	if len(events) != 2 {
		t.Fatalf("expected 2 events after expiration, got %d", len(events))
	}

	if events[0].ID != "evt-2" {
		t.Fatalf("expected evt-2 to remain, got %s", events[0].ID)
	}

	if events[1].ID != "evt-3" {
		t.Fatalf("expected evt-3 to remain, got %s", events[1].ID)
	}
}

func TestWindowSeparatesKeys(t *testing.T) {
	window := internal.NewWindow(10 * time.Second)

	now := time.Now()

	window.Add(windowEvent("evt-1", "user-42", now))
	window.Add(windowEvent("evt-2", "user-99", now))

	if events := window.Get("user-42"); len(events) != 1 {
		t.Fatalf("expected 1 event for user-42, got %d", len(events))
	}

	if events := window.Get("user-99"); len(events) != 1 {
		t.Fatalf("expected 1 event for user-99, got %d", len(events))
	}
}
