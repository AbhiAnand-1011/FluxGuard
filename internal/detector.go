package internal

import "time"

type Anomaly struct {
	EventID    string    `json:"event_id"`
	Key        string    `json:"key"`
	Rules      []string  `json:"rules"`
	DetectedAt time.Time `json:"detected_at"`
}

type Detector struct {
	engine *RuleEngine
}

func NewDetector(engine *RuleEngine) *Detector {
	return &Detector{
		engine: engine,
	}
}

func (d *Detector) Detect(events []Event) *Anomaly {
	if len(events) == 0 {
		return nil
	}

	matches := d.engine.Evaluate(events)

	if len(matches) == 0 {
		return nil
	}

	latest := events[len(events)-1]

	return &Anomaly{
		EventID:    latest.ID,
		Key:        latest.Key,
		Rules:      matches,
		DetectedAt: time.Now().UTC(),
	}
}
