package pool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/frame"
)

func startEchoServer(t testing.TB) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				defer c.Close()
				for {
					msg, err := frame.ReadFrame(c)
					if err != nil {
						return
					}
					if err := frame.WriteFrame(c, msg); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
		wg.Wait()
	}
}

func TestReuseAndLIFOOrder(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	var dials atomic.Int32
	p, err := New(Config{
		Addr:    addr,
		MaxOpen: 2,
		DialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	ctx := context.Background()
	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}
	c2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get c2: %v", err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}

	p.Put(c1)
	p.Put(c2)

	got1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get reuse 1: %v", err)
	}
	got2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get reuse 2: %v", err)
	}
	if got1 != c2 || got2 != c1 {
		t.Fatalf("expected lifo order (c2 then c1)")
	}
	if dials.Load() != 2 {
		t.Fatalf("expected zero new dials on reuse, got %d", dials.Load())
	}

	p.Put(got1)
	p.Put(got2)
}

func TestMaxOpenBlocksAndRespectsContext(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	p, err := New(Config{Addr: addr, MaxOpen: 1})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if _, err := p.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	if st := p.Stats(); st.Open != 1 || st.InUse != 1 || st.Waiters != 0 {
		t.Fatalf("unexpected stats after timeout: %+v", st)
	}

	done := make(chan net.Conn, 1)
	go func() {
		c, err := p.Get(context.Background())
		if err != nil {
			t.Errorf("blocked Get failed: %v", err)
			return
		}
		done <- c
	}()

	time.Sleep(15 * time.Millisecond)
	p.Put(c1)

	got := <-done
	if got != c1 {
		t.Fatalf("waiter did not receive handed-off connection")
	}
	p.Put(got)
}

func TestIdleTimeoutEviction(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	var dials atomic.Int32
	p, err := New(Config{
		Addr:        addr,
		MaxOpen:     2,
		IdleTimeout: 25 * time.Millisecond,
		DialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}
	p.Put(c1)

	time.Sleep(40 * time.Millisecond)

	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c2: %v", err)
	}
	defer p.Put(c2)

	if c2 == c1 {
		t.Fatalf("expected stale connection to be evicted")
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
	if st := p.Stats(); st.Open != 1 || st.InUse != 1 {
		t.Fatalf("unexpected stats after eviction: %+v", st)
	}
}

func TestDiscardUnblocksWaiterAndFreesSlot(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	var dials atomic.Int32
	p, err := New(Config{
		Addr:    addr,
		MaxOpen: 1,
		DialFunc: func(ctx context.Context, addr string) (net.Conn, error) {
			dials.Add(1)
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}

	gotCh := make(chan net.Conn, 1)
	go func() {
		c, err := p.Get(context.Background())
		if err != nil {
			t.Errorf("waiter Get failed: %v", err)
			return
		}
		gotCh <- c
	}()

	time.Sleep(15 * time.Millisecond)
	p.Discard(c1)

	c2 := <-gotCh
	if c2 == c1 {
		t.Fatalf("expected waiter to dial a fresh connection after Discard")
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
	p.Put(c2)
}

func TestPutResetsDeadline(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	p, err := New(Config{Addr: addr, MaxOpen: 1})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}
	if err := c1.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	p.Put(c1)

	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c2: %v", err)
	}
	defer p.Put(c2)

	want := []byte("hello-after-deadline-reset")
	if err := frame.WriteFrame(c2, want); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got, err := frame.ReadFrame(c2)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCloseUnblocksWaiters(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	p, err := New(Config{Addr: addr, MaxOpen: 1})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get c1: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Get(context.Background())
		errCh <- err
	}()

	time.Sleep(15 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := <-errCh; !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}

	// returning in-flight conn after close must close it and drop open to 0
	p.Put(c1)
	if st := p.Stats(); st.Open != 0 || st.Idle != 0 {
		t.Fatalf("expected 0 open after Close+Put, got %+v", st)
	}
}

func TestConcurrentStress(t *testing.T) {
	addr, cleanup := startEchoServer(t)
	defer cleanup()

	p, err := New(Config{Addr: addr, MaxOpen: 4, MaxIdle: 2})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer p.Close()

	const workers = 16
	const iters = 30
	payload := []byte("ping")

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range iters {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				conn, err := p.Get(ctx)
				cancel()
				if err != nil {
					t.Errorf("worker %d iter %d Get: %v", id, i, err)
					return
				}

				if err := frame.WriteFrame(conn, payload); err != nil {
					p.Discard(conn)
					t.Errorf("worker %d iter %d WriteFrame: %v", id, i, err)
					return
				}
				got, err := frame.ReadFrame(conn)
				if err != nil && !errors.Is(err, io.EOF) {
					p.Discard(conn)
					t.Errorf("worker %d iter %d ReadFrame: %v", id, i, err)
					return
				}
				if !bytes.Equal(got, payload) {
					p.Discard(conn)
					t.Errorf("worker %d iter %d mismatch: %q", id, i, got)
					return
				}

				// periodically discard to exercise concurrent slot handoffs
				if (id+i)%11 == 0 {
					p.Discard(conn)
				} else {
					p.Put(conn)
				}
			}
		}(w)
	}
	wg.Wait()

	if st := p.Stats(); st.InUse != 0 || st.Waiters != 0 || st.Idle > 2 {
		t.Fatalf("unexpected final stats: %+v", st)
	}
}

func BenchmarkPooledVsUnpooledEcho(b *testing.B) {
	addr, cleanup := startEchoServer(b)
	defer cleanup()

	payload := []byte("SET key value")

	b.Run("Pooled", func(b *testing.B) {
		p, err := New(Config{Addr: addr, MaxOpen: 4})
		if err != nil {
			b.Fatal(err)
		}
		defer p.Close()

		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			conn, err := p.Get(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if err := frame.WriteFrame(conn, payload); err != nil {
				p.Discard(conn)
				b.Fatal(err)
			}
			if _, err := frame.ReadFrame(conn); err != nil {
				p.Discard(conn)
				b.Fatal(err)
			}
			p.Put(conn)
		}
	})

	b.Run("Unpooled", func(b *testing.B) {
		dialer := &net.Dialer{}
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			conn, err := dialer.Dial("tcp", addr)
			if err != nil {
				b.Fatal(err)
			}
			if err := frame.WriteFrame(conn, payload); err != nil {
				_ = conn.Close()
				b.Fatal(err)
			}
			if _, err := frame.ReadFrame(conn); err != nil {
				_ = conn.Close()
				b.Fatal(err)
			}
			_ = conn.Close()
		}
	})
}
