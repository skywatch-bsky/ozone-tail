package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/skywatch-bsky/label-consumer/internal/config"
	"github.com/skywatch-bsky/label-consumer/internal/consumer"
	"github.com/skywatch-bsky/label-consumer/internal/storage"
)

func main() {
	log.Println("label-consumer starting")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	log.Printf("config loaded: labelers=%v", cfg.Labelers)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := storage.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := storage.EnsureSchema(ctx, pool); err != nil {
		log.Fatalf("failed to ensure schema: %v", err)
	}

	log.Println("schema ready")

	// Phase 2: single labeler only (first in list)
	c := &consumer.Consumer{
		Host:          cfg.Labelers[0],
		Pool:          pool,
		InitialCursor: cfg.InitialCursor,
	}

	if err := c.Run(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
