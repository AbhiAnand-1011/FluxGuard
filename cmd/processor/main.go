package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AbhiAnand-1011/FluxGuard/internal"
)

func main() {
	config := internal.LoadConfig()

	processor := internal.NewProcessor(
		config.KafkaBroker,
		config.KafkaTopic,
		config.KafkaAnomalyTopic,
		"fluxguard-processors",
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Println("FluxGuard processor started")

	if err := processor.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("processor error: %v", err)
	}

	if err := processor.Close(); err != nil {
		log.Fatalf("processor close error: %v", err)
	}

	log.Println("FluxGuard processor stopped")
}
