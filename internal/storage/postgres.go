package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS labels (
    src  TEXT NOT NULL,
    uri  TEXT NOT NULL,
    val  TEXT NOT NULL,
    cid  TEXT,
    cts  TIMESTAMPTZ,
    exp  TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (src, uri, val)
);

CREATE INDEX IF NOT EXISTS idx_labels_uri ON labels (uri);

CREATE TABLE IF NOT EXISTS label_consumer_cursors (
    labeler_host TEXT PRIMARY KEY,
    cursor       BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func Connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return pool, nil
}

func EnsureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}
	return nil
}
