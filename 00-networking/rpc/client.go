package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/frame"
	"github.com/avichal-08/distributed-systems-in-go/00-networking/pool"
)

var ErrCorrelationMismatch = errors.New("rpc: response correlation id mismatch")

type ClientOption func(*Client)

func WithClientCodec(c Codec) ClientOption         { return func(cl *Client) { cl.codec = c } }
func WithClientFramer(f frame.Framer) ClientOption { return func(cl *Client) { cl.framer = f } }

type Client struct {
	pool   *pool.Pool
	codec  Codec
	framer frame.Framer
	nextID atomic.Uint64
}

func NewClient(poolCfg pool.Config, opts ...ClientOption) (*Client, error) {
	p, err := pool.New(poolCfg)
	if err != nil {
		return nil, err
	}
	c := &Client{
		pool:  p,
		codec: JSONCodec{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Call executes an RPC. Returns *AppError on server-side domain errors (socket reused),
// or a transport/context error on wire failures (socket discarded).
func (c *Client) Call(ctx context.Context, method string, args any, reply any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var rawArgs json.RawMessage
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			return fmt.Errorf("rpc: marshal args: %w", err)
		}
		rawArgs = b
	}

	reqID := c.nextID.Add(1)
	reqBytes, err := c.codec.EncodeRequest(Request{
		ID:      reqID,
		Method:  method,
		Payload: rawArgs,
	})
	if err != nil {
		return fmt.Errorf("rpc: encode request: %w", err)
	}

	conn, err := c.pool.Get(ctx)
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			c.pool.Discard(conn)
			return err
		}
	}

	// wake blocked socket io immediately if ctx is cancelled without a deadline
	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})

	if err := c.framer.WriteFrame(conn, reqBytes); err != nil {
		stopCancel()
		c.pool.Discard(conn)
		return preferCtxErr(ctx, err)
	}

	respBytes, err := c.framer.ReadFrame(conn)
	if err != nil {
		stopCancel()
		c.pool.Discard(conn)
		return preferCtxErr(ctx, err)
	}

	// disarm cancellation hook before returning conn to pool
	stopCancel()

	resp, err := c.codec.DecodeResponse(respBytes)
	if err != nil {
		c.pool.Discard(conn)
		return fmt.Errorf("rpc: decode response: %w", err)
	}

	if resp.ID != reqID {
		c.pool.Discard(conn)
		return fmt.Errorf("%w: got %d, want %d", ErrCorrelationMismatch, resp.ID, reqID)
	}

	// wire exchange is clean; safe to reuse socket even if resp.Error is set
	c.pool.Put(conn)

	if resp.Error != "" {
		return &AppError{Message: resp.Error}
	}

	if reply != nil && len(resp.Payload) > 0 {
		if err := json.Unmarshal(resp.Payload, reply); err != nil {
			return fmt.Errorf("rpc: unmarshal reply: %w", err)
		}
	}
	return nil
}

func (c *Client) PoolStats() pool.Stats { return c.pool.Stats() }
func (c *Client) Close() error          { return c.pool.Close() }

func preferCtxErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	// netpoller deadline timer can fire microseconds before context's timer
	if _, hasDeadline := ctx.Deadline(); hasDeadline && errors.Is(err, os.ErrDeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return err
}
