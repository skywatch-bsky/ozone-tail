package main

import (
	"log"

	"github.com/skywatch-bsky/label-consumer/internal/config"
)

func main() {
	log.Println("label-consumer starting")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	log.Printf("config loaded: labelers=%v", cfg.Labelers)
}
