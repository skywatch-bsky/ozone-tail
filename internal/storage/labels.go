package storage

import (
	"context"
	"fmt"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ProcessLabelBatch(ctx context.Context, pool *pgxpool.Pool, labelerHost string, seq int64, labels []*comatproto.LabelDefs_Label) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, label := range labels {
		neg := label.Neg != nil && *label.Neg
		if neg {
			if err := deleteLabel(ctx, tx, label); err != nil {
				return fmt.Errorf("deleting label: %w", err)
			}
		} else {
			if err := upsertLabel(ctx, tx, label); err != nil {
				return fmt.Errorf("upserting label: %w", err)
			}
		}
	}

	if err := updateCursor(ctx, tx, labelerHost, seq); err != nil {
		return fmt.Errorf("updating cursor: %w", err)
	}

	return tx.Commit(ctx)
}

func upsertLabel(ctx context.Context, tx pgx.Tx, label *comatproto.LabelDefs_Label) error {
	var exp *time.Time
	if label.Exp != nil {
		parsed, err := time.Parse(time.RFC3339, *label.Exp)
		if err != nil {
			return fmt.Errorf("parsing exp timestamp %q: %w", *label.Exp, err)
		}
		exp = &parsed
	}

	var cts *time.Time
	if label.Cts != "" {
		parsed, err := time.Parse(time.RFC3339, label.Cts)
		if err != nil {
			return fmt.Errorf("parsing cts timestamp %q: %w", label.Cts, err)
		}
		cts = &parsed
	}

	_, err := tx.Exec(ctx, `
		INSERT INTO labels (src, uri, val, cid, cts, exp)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (src, uri, val) DO UPDATE SET
			cid = EXCLUDED.cid,
			cts = EXCLUDED.cts,
			exp = EXCLUDED.exp
	`, label.Src, label.Uri, label.Val, label.Cid, cts, exp)
	return err
}

func deleteLabel(ctx context.Context, tx pgx.Tx, label *comatproto.LabelDefs_Label) error {
	_, err := tx.Exec(ctx,
		"DELETE FROM labels WHERE src = $1 AND uri = $2 AND val = $3",
		label.Src, label.Uri, label.Val,
	)
	return err
}

func updateCursor(ctx context.Context, tx pgx.Tx, labelerHost string, seq int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO label_consumer_cursors (labeler_host, cursor, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (labeler_host) DO UPDATE SET
			cursor = EXCLUDED.cursor,
			updated_at = now()
	`, labelerHost, seq)
	return err
}
