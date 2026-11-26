package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/pool"
	"github.com/avichal-08/distributed-systems-in-go/00-networking/proxy"
	"github.com/avichal-08/distributed-systems-in-go/00-networking/rpc"
)

type kvSetArgs struct {
	Key   string `json:"key"`
	Value int    `json:"value"`
}

type kvGetArgs struct {
	Key string `json:"key"`
}

type kvStore struct {
	mu   sync.RWMutex
	data map[string]int
}

func (kv *kvStore) register(srv *rpc.Server) {
	srv.Register("KV.Set", func(ctx context.Context, payload []byte) ([]byte, error) {
		var args kvSetArgs
		if err := json.Unmarshal(payload, &args); err != nil {
			return nil, err
		}
		kv.mu.Lock()
		kv.data[args.Key] = args.Value
		kv.mu.Unlock()
		return json.Marshal("ok")
	})

	srv.Register("KV.Get", func(ctx context.Context, payload []byte) ([]byte, error) {
		var args kvGetArgs
		if err := json.Unmarshal(payload, &args); err != nil {
			return nil, err
		}
		kv.mu.RLock()
		v, ok := kv.data[args.Key]
		kv.mu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("key %q not found", args.Key)
		}
		return json.Marshal(v)
	})

	srv.Register("KV.Incr", func(ctx context.Context, payload []byte) ([]byte, error) {
		var args kvGetArgs
		if err := json.Unmarshal(payload, &args); err != nil {
			return nil, err
		}
		kv.mu.Lock()
		kv.data[args.Key]++
		v := kv.data[args.Key]
		kv.mu.Unlock()
		return json.Marshal(v)
	})
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store := &kvStore{data: make(map[string]int)}
	srv := rpc.NewServer(rpc.WithServerLogger(logger))
	store.register(srv)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	px := proxy.New("127.0.0.1:0", ln.Addr().String(), proxy.FaultConfig{}, logger)
	if err := px.Start(); err != nil {
		panic(err)
	}
	defer px.Close()

	cli, err := rpc.NewClient(pool.Config{Addr: px.Addr(), MaxOpen: 2})
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	fmt.Println("\n--- 1. healthy rpc ---")
	_ = cli.Call(context.Background(), "KV.Set", kvSetArgs{Key: "balance", Value: 100}, nil)
	var val int
	if err := cli.Call(context.Background(), "KV.Get", kvGetArgs{Key: "balance"}, &val); err != nil {
		panic(err)
	}
	fmt.Printf("KV.Get(balance) = %d | pool=%+v\n", val, cli.PoolStats())

	fmt.Println("\n--- 2. application error (socket reused) ---")
	err = cli.Call(context.Background(), "KV.Get", kvGetArgs{Key: "missing"}, &val)
	var appErr *rpc.AppError
	fmt.Printf("err=%v | isAppError=%v | pool=%+v\n", err, errors.As(err, &appErr), cli.PoolStats())

	fmt.Println("\n--- 3. blackhole partition (request never reaches server) ---")
	px.SetFault(proxy.FaultConfig{Blackhole: true})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err = cli.Call(ctx, "KV.Incr", kvGetArgs{Key: "balance"}, &val)
	cancel()
	fmt.Printf("client saw: %v | pool=%+v\n", err, cli.PoolStats())
	fmt.Printf("ground truth on server: balance = %d\n", store.data["balance"])

	fmt.Println("\n--- 4. mid-stream reset on response (partial-failure ambiguity) ---")
	px.SetFault(proxy.FaultConfig{ResetAfterBytes: 2})
	err = cli.Call(context.Background(), "KV.Incr", kvGetArgs{Key: "balance"}, &val)
	fmt.Printf("client saw: %v | pool=%+v\n", err, cli.PoolStats())
	fmt.Printf("ground truth on server: balance = %d (committed despite client error!)\n", store.data["balance"])
}
