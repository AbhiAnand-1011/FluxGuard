package internal

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Processor struct {
	reader           *kafka.Reader
	window           *Window
	detector         *Detector
	anomalyPublisher AnomalyPublisher
}

func NewProcessor(
	broker string,
	topic string,
	anomalyTopic string,
	groupID string,
) *Processor {
	rules := NewRuleEngine()
	anomalyPublisher := NewKafkaAnomalyPublisher(broker, anomalyTopic)

	return &Processor{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{broker},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
		}),
		window:           NewWindow(10 * time.Second),
		detector:         NewDetector(rules),
		anomalyPublisher: anomalyPublisher,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	for {
		message, err := p.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		var event Event

		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("failed to decode event: %v", err)

			if err := p.reader.CommitMessages(ctx, message); err != nil {
				return err
			}

			continue
		}

		events := p.window.Add(event)
		anomaly := p.detector.Detect(events)

		log.Printf(
			"processing event id=%s type=%s key=%s value=%f window_size=%d",
			event.ID,
			event.Type,
			event.Key,
			event.Value,
			len(events),
		)

		if anomaly != nil {
			if err := p.anomalyPublisher.PublishAnomaly(ctx, anomaly); err != nil {
				return err
			}

			log.Printf(
				"anomaly detected event_id=%s key=%s rules=%v",
				anomaly.EventID,
				anomaly.Key,
				anomaly.Rules,
			)
		}

		if err := p.reader.CommitMessages(ctx, message); err != nil {
			return err
		}
	}
}

func (p *Processor) Close() error {
	if err := p.reader.Close(); err != nil {
		return err
	}

	return p.anomalyPublisher.Close()
}
