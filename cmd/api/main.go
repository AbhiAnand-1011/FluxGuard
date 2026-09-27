package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AbhiAnand-1011/FluxGuard/internal"
)

func main() {
	config := internal.LoadConfig()

	publisher := internal.NewKafkaPublisher(
		config.KafkaBroker,
		config.KafkaTopic,
	)
	defer publisher.Close()

	mux := http.NewServeMux()

	api := internal.NewAPI(publisher)
	api.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    ":" + config.HTTPPort,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		log.Printf("FluxGuard API listening on :%s", config.HTTPPort)

		if err := server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()

	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}

	log.Println("FluxGuard API stopped")
}
