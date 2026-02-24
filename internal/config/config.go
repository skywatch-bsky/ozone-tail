package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Labelers      []string
	PostgresDSN   string
	InitialCursor *int64
}

func Load() (*Config, error) {
	labelersJSON := os.Getenv("LABEL_CONSUMER_LABELERS")
	if labelersJSON == "" {
		return nil, fmt.Errorf("LABEL_CONSUMER_LABELERS is required")
	}

	var labelers []string
	if err := json.Unmarshal([]byte(labelersJSON), &labelers); err != nil {
		return nil, fmt.Errorf("parsing LABEL_CONSUMER_LABELERS: %w", err)
	}
	if len(labelers) == 0 {
		return nil, fmt.Errorf("LABEL_CONSUMER_LABELERS must contain at least one labeler")
	}

	dsn := os.Getenv("LABEL_CONSUMER_POSTGRES_DSN")
	if dsn == "" {
		return nil, fmt.Errorf("LABEL_CONSUMER_POSTGRES_DSN is required")
	}

	cfg := &Config{
		Labelers:    labelers,
		PostgresDSN: dsn,
	}

	if cursorStr := os.Getenv("LABEL_CONSUMER_INITIAL_CURSOR"); cursorStr != "" {
		var cursor int64
		if _, err := fmt.Sscanf(cursorStr, "%d", &cursor); err != nil {
			return nil, fmt.Errorf("parsing LABEL_CONSUMER_INITIAL_CURSOR: %w", err)
		}
		cfg.InitialCursor = &cursor
	}

	return cfg, nil
}
