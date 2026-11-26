package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/pool"
)

type addArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addReply struct {
	Sum int `json:"sum"`
}

func startTestServer(t testing.TB) (*Server, string, func()) {
	t.Helper()
	srv := NewServer()

	srv.Register("Math.Add", func(ctx context.Context, payload []byte) ([]byte, error) {
		var args addArgs
		if err := json.Unmarshal(payload, &args); err != nil {
			return nil, err
		}
		return json.Marshal(addReply{Sum: args.A + args.B})
	})

	srv.Register("Test.AppErr", func(ctx context.Context, payload []byte) ([]byte, error) {
		return nil, errors.New("key not found")
	})

	srv.Register("Test.Panic", func(ctx context.Context, payload []byte) ([]byte, error) {
		panic("boom")
	})

	srv.Register("Test.Slow", func(ctx context.Context, payload []byte) ([]byte, error) {
		time.Sleep(150 * time.Millisecond)
		return json.Marshal("done")
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

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-done
	}
	return srv, ln.Addr().String(), cleanup
}

func TestRoundTripAndAppErrorReusesConn(t *testing.T) {
	_, addr, cleanup := startTestServer(t)
	defer cleanup()

	cli, err := NewClient(pool.Config{Addr: addr, MaxOpen: 2})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	var reply addReply
	if err := cli.Call(context.Background(), "Math.Add", addArgs{A: 7, B: 5}, &reply); err != nil {
		t.Fatalf("Math.Add: %v", err)
	}
	if reply.Sum != 12 {
		t.Fatalf("sum = %d, want 12", reply.Sum)
	}

	// app error must be typed as *AppError and keep the socket in the pool
	err = cli.Call(context.Background(), "Test.AppErr", nil, nil)
	var appErr *AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected *AppError, got %T (%v)", err, err)
	}
	if appErr.Message != "key not found" {
		t.Fatalf("app error msg = %q", appErr.Message)
	}

	if st := cli.PoolStats(); st.Idle != 1 || st.Open != 1 {
		t.Fatalf("expected reused idle conn after app error, got %+v", st)
	}
}

func TestUnknownMethodAndPanicIsolation(t *testing.T) {
	_, addr, cleanup := startTestServer(t)
	defer cleanup()

	cli, err := NewClient(pool.Config{Addr: addr, MaxOpen: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	var appErr *AppError
	if err := cli.Call(context.Background(), "Does.NotExist", nil, nil); !errors.As(err, &appErr) {
		t.Fatalf("expected *AppError for unknown method, got %v", err)
	}

	if err := cli.Call(context.Background(), "Test.Panic", nil, nil); !errors.As(err, &appErr) {
		t.Fatalf("expected *AppError for handler panic, got %v", err)
	}

	// server and connection must remain healthy after panic
	var reply addReply
	if err := cli.Call(context.Background(), "Math.Add", addArgs{A: 1, B: 2}, &reply); err != nil {
		t.Fatalf("call after panic failed: %v", err)
	}
	if reply.Sum != 3 {
		t.Fatalf("sum = %d, want 3", reply.Sum)
	}
}

func TestTimeoutDiscardsConn(t *testing.T) {
	_, addr, cleanup := startTestServer(t)
	defer cleanup()

	cli, err := NewClient(pool.Config{Addr: addr, MaxOpen: 1})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err = cli.Call(ctx, "Test.Slow", nil, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// timed-out socket must be discarded so next caller never reads stale reply
	if st := cli.PoolStats(); st.Open != 0 || st.Idle != 0 {
		t.Fatalf("expected discarded conn (0 open), got %+v", st)
	}

	var reply addReply
	if err := cli.Call(context.Background(), "Math.Add", addArgs{A: 10, B: 20}, &reply); err != nil {
		t.Fatalf("subsequent call failed: %v", err)
	}
	if reply.Sum != 30 {
		t.Fatalf("sum = %d, want 30", reply.Sum)
	}
}

func TestConcurrentCalls(t *testing.T) {
	_, addr, cleanup := startTestServer(t)
	defer cleanup()

	cli, err := NewClient(pool.Config{Addr: addr, MaxOpen: 4})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cli.Close()

	var wg sync.WaitGroup
	for g := range 12 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 25 {
				var reply addReply
				if err := cli.Call(context.Background(), "Math.Add", addArgs{A: id, B: i}, &reply); err != nil {
					t.Errorf("call failed: %v", err)
					return
				}
				if reply.Sum != id+i {
					t.Errorf("got %d, want %d", reply.Sum, id+i)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkRPCPooled(b *testing.B) {
	_, addr, cleanup := startTestServer(b)
	defer cleanup()

	cli, err := NewClient(pool.Config{Addr: addr, MaxOpen: 4})
	if err != nil {
		b.Fatal(err)
	}
	defer cli.Close()

	args := addArgs{A: 10, B: 20}
	var reply addReply
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := cli.Call(ctx, "Math.Add", args, &reply); err != nil {
			b.Fatal(err)
		}
	}
}
