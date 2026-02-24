package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

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

	g, gctx := errgroup.WithContext(ctx)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: ":9090", Handler: mux}

	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("metrics server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		return srv.Shutdown(context.Background())
	})

	for _, host := range cfg.Labelers {
		// Go 1.22+ semantics: loop variables are per-iteration, so capturing
		// `host` in the closure below is safe without a local copy.
		c := &consumer.Consumer{
			Host:          host,
			Pool:          pool,
			InitialCursor: cfg.InitialCursor,
		}
		g.Go(func() error {
			return c.Run(gctx)
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
}
