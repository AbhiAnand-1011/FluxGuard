package internal

import "time"

type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Source    string    `json:"source"`
	Key       string    `json:"key"`
	Value     float64   `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}
