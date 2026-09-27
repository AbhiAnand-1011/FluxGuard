package tests

import (
	"testing"
	"time"

	"github.com/AbhiAnand-1011/FluxGuard/internal"
)

func testEvent(id string, value float64) internal.Event {
	return internal.Event{
		ID:        id,
		Type:      "transaction",
		Source:    "payment-service",
		Key:       "user-42",
		Value:     value,
		Timestamp: time.Now(),
	}
}

func TestDetectorHighEventCount(t *testing.T) {
	engine := internal.NewRuleEngine()
	detector := internal.NewDetector(engine)
	window := internal.NewWindow(10 * time.Second)

	var events []internal.Event

	for i := 1; i <= 5; i++ {
		event := testEvent("evt-"+string(rune('0'+i)), 100)
		events = window.Add(event)
	}

	anomaly := detector.Detect(events)

	if anomaly == nil {
		t.Fatal("expected anomaly, got nil")
	}

	if len(anomaly.Rules) != 1 || anomaly.Rules[0] != "high_event_count" {
		t.Fatalf("unexpected rules: %v", anomaly.Rules)
	}
}

func TestDetectorHighTotalValue(t *testing.T) {
	engine := internal.NewRuleEngine()
	detector := internal.NewDetector(engine)
	window := internal.NewWindow(10 * time.Second)

	var events []internal.Event

	for i := 1; i <= 3; i++ {
		event := testEvent("evt-"+string(rune('0'+i)), 2000)
		events = window.Add(event)
	}

	anomaly := detector.Detect(events)

	if anomaly == nil {
		t.Fatal("expected anomaly, got nil")
	}

	found := false

	for _, rule := range anomaly.Rules {
		if rule == "high_total_value" {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected high_total_value, got %v", anomaly.Rules)
	}
}

func TestDetectorNormalActivity(t *testing.T) {
	engine := internal.NewRuleEngine()
	detector := internal.NewDetector(engine)
	window := internal.NewWindow(10 * time.Second)

	var events []internal.Event

	for i := 1; i <= 2; i++ {
		event := testEvent("evt-"+string(rune('0'+i)), 100)
		events = window.Add(event)
	}

	anomaly := detector.Detect(events)

	if anomaly != nil {
		t.Fatalf("expected no anomaly, got %v", anomaly)
	}
}
