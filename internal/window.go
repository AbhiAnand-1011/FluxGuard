package internal

import (
	"sync"
	"time"
)

type Window struct {
	duration time.Duration
	mu       sync.RWMutex
	events   map[string][]Event
}

func NewWindow(duration time.Duration) *Window {
	return &Window{
		duration: duration,
		events:   make(map[string][]Event),
	}
}

func (w *Window) Add(event Event) []Event {
	w.mu.Lock()
	defer w.mu.Unlock()

	current := w.events[event.Key]
	cutoff := event.Timestamp.Add(-w.duration)

	filtered := current[:0]

	for _, existing := range current {
		if !existing.Timestamp.Before(cutoff) {
			filtered = append(filtered, existing)
		}
	}

	filtered = append(filtered, event)
	w.events[event.Key] = filtered

	return append([]Event(nil), filtered...)
}

func (w *Window) Get(key string) []Event {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return append([]Event(nil), w.events[key]...)
}
