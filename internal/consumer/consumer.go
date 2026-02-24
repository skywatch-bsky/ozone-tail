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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		mt, r, err := conn.NextReader()
		if err != nil {
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
			if err := c.handleMessage(header.MsgType, r); err != nil {
				return err
			}
		case events.EvtKindErrorFrame:
			return fmt.Errorf("received error frame from %s", c.Host)
		default:
			log.Printf("[%s] unknown op=%d", c.Host, header.Op)
		}
	}
}

func (c *Consumer) handleMessage(msgType string, r io.Reader) error {
	switch msgType {
	case "#labels":
		var evt comatproto.LabelSubscribeLabels_Labels
		if err := evt.UnmarshalCBOR(r); err != nil {
			return fmt.Errorf("decoding #labels: %w", err)
		}
		for _, label := range evt.Labels {
			neg := label.Neg != nil && *label.Neg
			log.Printf("[%s] seq=%d src=%s uri=%s val=%s neg=%v",
				c.Host, evt.Seq, label.Src, label.Uri, label.Val, neg)
		}
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
