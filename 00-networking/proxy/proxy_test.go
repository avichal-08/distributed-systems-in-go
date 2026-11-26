package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/pool"
	"github.com/avichal-08/distributed-systems-in-go/00-networking/rpc"
)

func startKVServer(t *testing.T) (string, *atomic.Int64, func()) {
	t.Helper()
	var counter atomic.Int64

	srv := rpc.NewServer()
	srv.Register("Counter.Incr", func(ctx context.Context, payload []byte) ([]byte, error) {
		val := counter.Add(1)
		return json.Marshal(val)
	})
	srv.Register("Echo.Bytes", func(ctx context.Context, payload []byte) ([]byte, error) {
		return payload, nil
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()

	return ln.Addr().String(), &counter, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}
}

func TestPassthroughAndLatency(t *testing.T) {
	targetAddr, _, stopSrv := startKVServer(t)
	defer stopSrv()

	px := New("127.0.0.1:0", targetAddr, FaultConfig{Latency: 40 * time.Millisecond}, nil)
	if err := px.Start(); err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer px.Close()

	cli, err := rpc.NewClient(pool.Config{Addr: px.Addr(), MaxOpen: 2})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	start := time.Now()
	var val int64
	if err := cli.Call(context.Background(), "Counter.Incr", nil, &val); err != nil {
		t.Fatalf("Incr: %v", err)
	}
	elapsed := time.Since(start)

	if val != 1 {
		t.Fatalf("val = %d, want 1", val)
	}
	// request + response both cross the proxy => >= 80ms
	if elapsed < 70*time.Millisecond {
		t.Fatalf("expected >= 70ms latency, got %v", elapsed)
	}
}

func TestBandwidthThrottle(t *testing.T) {
	targetAddr, _, stopSrv := startKVServer(t)
	defer stopSrv()

	px := New("127.0.0.1:0", targetAddr, FaultConfig{BandwidthBps: 40 * 1024}, nil) // 40 KiB/s
	if err := px.Start(); err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer px.Close()

	cli, err := rpc.NewClient(pool.Config{Addr: px.Addr(), MaxOpen: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	// 4 KiB echoed both ways (~8 KiB total) at 40 KiB/s => ~200ms
	msg := string(bytes.Repeat([]byte("a"), 4096))
	var reply string

	start := time.Now()
	if err := cli.Call(context.Background(), "Echo.Bytes", msg, &reply); err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if reply != msg {
		t.Fatalf("reply mismatch")
	}
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("expected throttled transfer >= 120ms, got %v", elapsed)
	}
}

func TestBlackholeCausesTimeout(t *testing.T) {
	targetAddr, counter, stopSrv := startKVServer(t)
	defer stopSrv()

	px := New("127.0.0.1:0", targetAddr, FaultConfig{Blackhole: true}, nil)
	if err := px.Start(); err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer px.Close()

	cli, err := rpc.NewClient(pool.Config{Addr: px.Addr(), MaxOpen: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	var val int64
	err = cli.Call(ctx, "Counter.Incr", nil, &val)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded under blackhole, got %v", err)
	}
	if counter.Load() != 0 {
		t.Fatalf("server should not have received blackholed request, counter=%d", counter.Load())
	}
}

func TestMidStreamResetShowsPartialFailureAmbiguity(t *testing.T) {
	targetAddr, counter, stopSrv := startKVServer(t)
	defer stopSrv()

	// reset after 2 bytes of the server's response header
	px := New("127.0.0.1:0", targetAddr, FaultConfig{ResetAfterBytes: 2}, nil)
	if err := px.Start(); err != nil {
		t.Fatalf("proxy start: %v", err)
	}
	defer px.Close()

	cli, err := rpc.NewClient(pool.Config{Addr: px.Addr(), MaxOpen: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	var val int64
	err = cli.Call(context.Background(), "Counter.Incr", nil, &val)
	if err == nil {
		t.Fatalf("expected transport error on mid-stream reset, got nil")
	}

	var appErr *rpc.AppError
	if errors.As(err, &appErr) {
		t.Fatalf("expected transport error, got AppError: %v", err)
	}

	// despite client error, the server already executed and committed the increment
	if counter.Load() != 1 {
		t.Fatalf("expected server counter == 1 (partial failure), got %d", counter.Load())
	}
}
