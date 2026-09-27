package internal

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type EventPublisher interface {
	Publish(ctx context.Context, event Event) error
}

type AnomalyPublisher interface {
	PublishAnomaly(ctx context.Context, anomaly *Anomaly) error
	Close() error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(broker, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.Key),
		Value: data,
	})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

type KafkaAnomalyPublisher struct {
	writer *kafka.Writer
}

func NewKafkaAnomalyPublisher(broker, topic string) *KafkaAnomalyPublisher {
	return &KafkaAnomalyPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *KafkaAnomalyPublisher) PublishAnomaly(
	ctx context.Context,
	anomaly *Anomaly,
) error {
	data, err := json.Marshal(anomaly)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(anomaly.Key),
		Value: data,
	})
}

func (p *KafkaAnomalyPublisher) Close() error {
	return p.writer.Close()
}
