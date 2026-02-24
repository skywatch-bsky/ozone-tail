package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/skywatch-bsky/label-consumer/internal/storage"
)

type Consumer struct {
	Host          string
	Pool          *pgxpool.Pool
	InitialCursor *int64
}

func (c *Consumer) Run(ctx context.Context) error {
	cursor, err := c.resolveCursor(ctx)
	if err != nil {
		return fmt.Errorf("resolving cursor: %w", err)
	}

	conn, err := c.dial(ctx, cursor)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", c.Host, err)
	}
	defer conn.Close()

	log.Printf("[%s] connected, cursor=%v", c.Host, cursor)

	return c.readLoop(ctx, conn)
}

func (c *Consumer) resolveCursor(ctx context.Context) (*int64, error) {
	cursor, err := storage.ReadCursor(ctx, c.Pool, c.Host)
	if err != nil {
		return nil, err
	}
	if cursor != nil {
		return cursor, nil
	}
	return c.InitialCursor, nil
}

func (c *Consumer) dial(ctx context.Context, cursor *int64) (*websocket.Conn, error) {
	u := url.URL{
		Scheme: "wss",
		Host:   c.Host,
		Path:   "/xrpc/com.atproto.label.subscribeLabels",
	}
	if cursor != nil {
		q := u.Query()
		q.Set("cursor", fmt.Sprintf("%d", *cursor))
		u.RawQuery = q.Encode()
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Consumer) readLoop(ctx context.Context, conn *websocket.Conn) error {
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	for {
		mt, r, err := conn.NextReader()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("reading message: %w", err)
		}

		if mt != websocket.BinaryMessage {
			log.Printf("[%s] skipping non-binary message type=%d", c.Host, mt)
			continue
		}

		var header events.EventHeader
		if err := header.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("decoding header: %w", err)
		}

		switch header.Op {
		case events.EvtKindMessage:
			if err := c.handleMessage(ctx, header.MsgType, r); err != nil {
				return err
			}
		case events.EvtKindErrorFrame:
			var errframe events.ErrorFrame
			if err := errframe.UnmarshalCBOR(r); err != nil {
				return fmt.Errorf("decoding error frame from %s: %w", c.Host, err)
			}
			return fmt.Errorf("error frame from %s: %s: %s", c.Host, errframe.Error, errframe.Message)
		default:
			log.Printf("[%s] unknown op=%d", c.Host, header.Op)
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msgType string, r io.Reader) error {
	switch msgType {
	case "#labels":
		var evt comatproto.LabelSubscribeLabels_Labels
		if err := evt.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("decoding #labels: %w", err)
		}
		if err := storage.ProcessLabelBatch(ctx, c.Pool, c.Host, evt.Seq, evt.Labels); err != nil {
			return fmt.Errorf("processing label batch seq=%d: %w", evt.Seq, err)
		}
		log.Printf("[%s] processed seq=%d labels=%d", c.Host, evt.Seq, len(evt.Labels))
	case "#info":
		var info comatproto.LabelSubscribeLabels_Info
		if err := info.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("decoding #info: %w", err)
		}
		msg := ""
		if info.Message != nil {
			msg = *info.Message
		}
		log.Printf("[%s] info: name=%s message=%s", c.Host, info.Name, msg)
	default:
		log.Printf("[%s] unhandled message type: %s", c.Host, msgType)
	}
	return nil
}
