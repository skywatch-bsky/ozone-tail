# ozone-tail

Tails AT Protocol `subscribeLabels` WebSocket firehoses and persists labels to Postgres. Designed as a sidecar for [Osprey](https://github.com/skywatch-bsky/skywatch-osprey), replacing direct Ozone API reads with local database lookups.

## How it works

Each configured labeler gets its own goroutine that:

1. Connects to `wss://{host}/xrpc/com.atproto.label.subscribeLabels`
2. Decodes CBOR-encoded label events
3. Upserts/deletes labels in the `labels` table (composite PK: `src, uri, val`)
4. Persists cursor position for resume after restart

On disconnect, the process exits and relies on container restart policy to reconnect.

## Configuration

All configuration is via environment variables.

| Variable | Required | Description |
|----------|----------|-------------|
| `LABEL_CONSUMER_LABELERS` | yes | JSON array of labeler hostnames, e.g. `'["ozone.skywatch.blue","labeler.hailey.at"]'` |
| `LABEL_CONSUMER_POSTGRES_DSN` | yes | Postgres connection string, e.g. `postgres://user:pass@localhost:5432/osprey` |
| `LABEL_CONSUMER_INITIAL_CURSOR` | no | Sequence number to start from on first run (omit to start from live) |

## Database schema

Created automatically on startup via `CREATE TABLE IF NOT EXISTS`:

```sql
-- Labels from subscribeLabels firehose
CREATE TABLE labels (
    src  TEXT NOT NULL,          -- labeler DID
    uri  TEXT NOT NULL,          -- subject AT URI or DID
    val  TEXT NOT NULL,          -- label value
    cid  TEXT,                   -- content CID (nullable)
    cts  TIMESTAMPTZ,            -- label creation timestamp
    exp  TIMESTAMPTZ,            -- expiration (null = permanent)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (src, uri, val)
);

-- Cursor tracking per labeler
CREATE TABLE label_consumer_cursors (
    labeler_host TEXT PRIMARY KEY,
    cursor       BIGINT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Running

### Locally

```bash
export LABEL_CONSUMER_LABELERS='["ozone.skywatch.blue"]'
export LABEL_CONSUMER_POSTGRES_DSN='postgres://localhost:5432/osprey'
go run .
```

### Docker

```bash
docker build -t label-consumer .
docker run \
  -e LABEL_CONSUMER_LABELERS='["ozone.skywatch.blue"]' \
  -e LABEL_CONSUMER_POSTGRES_DSN='postgres://user:pass@host:5432/osprey' \
  label-consumer
```

### docker-compose (with Osprey)

See `docker-osprey-compose.yaml` (coming soon). The service is configured with `restart: unless-stopped`.

## Prometheus metrics

Exposed on `:9090/metrics`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `label_consumer_labels_processed_total` | counter | `labeler_host`, `operation` | Labels processed (upsert/delete) |
| `label_consumer_connected` | gauge | `labeler_host` | Connection status (1/0) |
| `label_consumer_cursor_value` | gauge | `labeler_host` | Current cursor position |

## Project structure

```
main.go                         # entrypoint, signal handling, errgroup orchestration
internal/
  config/config.go              # env var parsing
  consumer/consumer.go          # WebSocket connection, CBOR decoding, message handling
  storage/
    postgres.go                 # connection, schema, cursor reads
    labels.go                   # transactional label upsert/delete + cursor update
  metrics/metrics.go            # Prometheus metric definitions
```
