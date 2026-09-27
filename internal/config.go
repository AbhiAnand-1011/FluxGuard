package internal

import "os"

type Config struct {
	HTTPPort          string
	KafkaBroker       string
	KafkaTopic        string
	KafkaAnomalyTopic string
}

func LoadConfig() Config {
	httpPort := os.Getenv("FLUXGUARD_HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	kafkaBroker := os.Getenv("FLUXGUARD_KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	kafkaTopic := os.Getenv("FLUXGUARD_KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "fluxguard-events"
	}

	kafkaAnomalyTopic := os.Getenv("FLUXGUARD_KAFKA_ANOMALY_TOPIC")
	if kafkaAnomalyTopic == "" {
		kafkaAnomalyTopic = "fluxguard-anomalies"
	}

	return Config{
		HTTPPort:          httpPort,
		KafkaBroker:       kafkaBroker,
		KafkaTopic:        kafkaTopic,
		KafkaAnomalyTopic: kafkaAnomalyTopic,
	}
}
