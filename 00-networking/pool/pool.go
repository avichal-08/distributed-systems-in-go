package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const DefaultDialTimeout = 5 * time.Second

var ErrPoolClosed = errors.New("pool: closed")

type Config struct {
	Addr        string
	MaxOpen     int
	MaxIdle     int
	IdleTimeout time.Duration // 0 disables idle eviction
	DialTimeout time.Duration
	DialFunc    func(ctx context.Context, addr string) (net.Conn, error)
}

type Stats struct {
	Open    int
	Idle    int
	InUse   int
	Waiters int
}

type idleConn struct {
	conn     net.Conn
	lastUsed time.Time
}

type waitResult struct {
	conn net.Conn // nil with nil err means a dial slot was transferred
	err  error
}

type waiter struct {
	ch chan waitResult
}

type Pool struct {
	cfg Config

	mu      sync.Mutex
	closed  bool
	open    int
	idle    []idleConn // lifo stack
	waiters []*waiter  // fifo queue
}

func New(cfg Config) (*Pool, error) {
	if cfg.Addr == "" && cfg.DialFunc == nil {
		return nil, errors.New("pool: Addr or DialFunc is required")
	}
	if cfg.MaxOpen <= 0 {
		return nil, fmt.Errorf("pool: MaxOpen must be > 0, got %d", cfg.MaxOpen)
	}
	if cfg.MaxIdle <= 0 || cfg.MaxIdle > cfg.MaxOpen {
		cfg.MaxIdle = cfg.MaxOpen
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.DialFunc == nil {
		dialer := &net.Dialer{Timeout: cfg.DialTimeout}
		cfg.DialFunc = func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", addr)
		}
	}

	return &Pool{
		cfg:  cfg,
		idle: make([]idleConn, 0, cfg.MaxIdle),
	}, nil
}

func (p *Pool) Get(ctx context.Context) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var expired []net.Conn

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}

	for len(p.idle) > 0 {
		last := len(p.idle) - 1
		entry := p.idle[last]
		p.idle[last] = idleConn{} // avoid retaining conn for gc
		p.idle = p.idle[:last]

		if p.cfg.IdleTimeout > 0 && time.Since(entry.lastUsed) > p.cfg.IdleTimeout {
			p.open--
			expired = append(expired, entry.conn)
			continue
		}

		p.mu.Unlock()
		closeAll(expired)
		return entry.conn, nil
	}

	// reserve slot under lock, dial outside lock
	if p.open < p.cfg.MaxOpen {
		p.open++
		p.mu.Unlock()
		closeAll(expired)
		return p.dialWithReservedSlot(ctx)
	}

	w := &waiter{ch: make(chan waitResult, 1)}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()
	closeAll(expired)

	select {
	case res := <-w.ch:
		if res.err != nil {
			return nil, res.err
		}
		if res.conn != nil {
			return res.conn, nil
		}
		return p.dialWithReservedSlot(ctx)

	case <-ctx.Done():
		p.mu.Lock()
		removed := p.removeWaiterLocked(w)
		p.mu.Unlock()

		// handoff won the race; drain channel so conn/slot isn't leaked
		if !removed {
			res := <-w.ch
			if res.conn != nil {
				p.Put(res.conn)
			} else if res.err == nil {
				p.releaseSlot()
			}
		}
		return nil, ctx.Err()
	}
}

func (p *Pool) Put(conn net.Conn) {
	if conn == nil {
		return
	}

	// clear per request deadline before reusing socket
	if err := conn.SetDeadline(time.Time{}); err != nil {
		p.Discard(conn)
		return
	}

	p.mu.Lock()
	if p.closed {
		p.open--
		p.mu.Unlock()
		_ = conn.Close()
		return
	}

	if len(p.waiters) > 0 {
		w := p.popWaiterLocked()
		w.ch <- waitResult{conn: conn}
		p.mu.Unlock()
		return
	}

	if len(p.idle) < p.cfg.MaxIdle {
		p.idle = append(p.idle, idleConn{
			conn:     conn,
			lastUsed: time.Now(),
		})
		p.mu.Unlock()
		return
	}

	p.open--
	p.mu.Unlock()
	_ = conn.Close()
}

func (p *Pool) Discard(conn net.Conn) {
	if conn != nil {
		_ = conn.Close()
	}
	p.releaseSlot()
}

func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Open:    p.open,
		Idle:    len(p.idle),
		InUse:   p.open - len(p.idle),
		Waiters: len(p.waiters),
	}
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	toClose := make([]net.Conn, len(p.idle))
	for i, entry := range p.idle {
		toClose[i] = entry.conn
	}
	p.open -= len(p.idle)
	p.idle = nil

	for _, w := range p.waiters {
		w.ch <- waitResult{err: ErrPoolClosed}
	}
	p.waiters = nil
	p.mu.Unlock()

	var errs []error
	for _, c := range toClose {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *Pool) dialWithReservedSlot(ctx context.Context) (net.Conn, error) {
	conn, err := p.cfg.DialFunc(ctx, p.cfg.Addr)
	if err != nil {
		p.releaseSlot()
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.open--
		p.mu.Unlock()
		_ = conn.Close()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()
	return conn, nil
}

func (p *Pool) releaseSlot() {
	p.mu.Lock()
	if !p.closed && len(p.waiters) > 0 {
		w := p.popWaiterLocked()
		w.ch <- waitResult{} // transfer dial slot to next waiter
		p.mu.Unlock()
		return
	}
	p.open--
	p.mu.Unlock()
}

func (p *Pool) popWaiterLocked() *waiter {
	w := p.waiters[0]
	copy(p.waiters, p.waiters[1:])
	p.waiters[len(p.waiters)-1] = nil
	p.waiters = p.waiters[:len(p.waiters)-1]
	return w
}

func (p *Pool) removeWaiterLocked(target *waiter) bool {
	for i, w := range p.waiters {
		if w == target {
			copy(p.waiters[i:], p.waiters[i+1:])
			p.waiters[len(p.waiters)-1] = nil
			p.waiters = p.waiters[:len(p.waiters)-1]
			return true
		}
	}
	return false
}

func closeAll(conns []net.Conn) {
	for _, c := range conns {
		_ = c.Close()
	}
}
