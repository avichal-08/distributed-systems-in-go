# 00-networking

Phase 0 of `distributed-systems-in-go`: a layered TCP networking stack built with zero third-party dependencies (Go standard library only), designed to put services on the wire and deliberately break the wire.

## Architecture

```text
+------------+     +------------+     +------------+
| RPC Client | --> | Conn Pool  | --> |   Framer   |
+------------+     | (bounded)  |     | (4B len+p) |
                   +------------+     +-----+------+
                                            |
                                    TCP Byte Stream
                                            |
                                      +-----v------+
                                      | Fault Proxy|  (latency, jitter, throttle,
                                      |  (optional)|   blackhole, mid-stream RST)
                                      +-----+------+
                                            |
                                      +-----v------+     +------------+
                                      |   Framer   | --> | RPC Server |
                                      +------------+     +------------+
```

## Packages

- **`frame/`** — 4-byte Big-Endian length-prefixed binary framing (`ReadFrame` / `WriteFrame`). Uses `net.Buffers` for vectored `writev` syscalls, `io.ReadFull` for fragmented reads, a `MaxFrameSize` OOM guard (default 16 MiB), and strict separation between clean boundary `io.EOF` and truncated `io.ErrUnexpectedEOF`.
- **`pool/`** — Thread-safe bounded TCP connection pool (`Get(ctx)` / `Put` / `Discard`). Implements a LIFO idle stack, lazy `IdleTimeout` eviction, FIFO waiters with context cancellation, deadline clearing on `Put`, and dialing outside the mutex.
- **`rpc/`** — Minimal JSON-over-frame RPC client and concurrent server with `uint64` correlation IDs, per-call `context.WithTimeout` bridged via `SetDeadline` + `context.AfterFunc`, panic isolation, graceful `Shutdown(ctx)`, and typed `*AppError` vs. transport error separation.
- **`proxy/`** — Transparent bidirectional TCP fault-injection proxy supporting latency/jitter, bandwidth throttling, silent blackholes, and mid-stream `TCP RST` (`SetLinger(0)`).
- **`cmd/`** — Live demo wiring `rpc.Server`, `proxy.Proxy`, and `rpc.Client` together to prove partial-failure ambiguity.

## Wire Protocol

Every message is wrapped in a length-prefixed binary frame:

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Payload Length (uint32 BE)                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   JSON Envelope (N bytes)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

**RPC Envelopes:**

```json
// Request
{"id": 1, "method": "KV.Get", "payload": {"key": "balance"}}

// Response
{"id": 1, "payload": 100, "error": ""}
```

## Failure Scenarios Demonstrated

| Scenario | Client Observation | Socket Action | Server State |
| :--- | :--- | :--- | :--- |
| Healthy RPC | `100` | `Put` (reused) | `balance = 100` |
| Application Error | `*rpc.AppError` | `Put` (reused) | `balance = 100` |
| Blackhole Partition | `context.DeadlineExceeded` | `Discard` (closed) | `balance = 100` (never received) |
| Mid-Stream Response `RST` | `connection reset by peer` | `Discard` (closed) | `balance = 101` (committed!) |

## Benchmarks

Measured on `linux/amd64` (Intel Core i3-1005G1 @ 1.20GHz, loopback):

| Benchmark | Latency | Memory | Allocs |
| :--- | ---: | ---: | ---: |
| `frame.WriteFrame` (64 B) | `455.9 ns/op` | `76 B/op` | `3 allocs/op` |
| `frame.WriteFrame` (64 KiB) | `393.7 ns/op` | `76 B/op` | `3 allocs/op` |
| `frame.ReadFrame` (64 B) | `307.4 ns/op` | `68 B/op` | `2 allocs/op` |
| `frame.ReadFrame` (64 KiB) | `59,287 ns/op` | `65,540 B/op` | `2 allocs/op` |
| `pool` Echo — **Pooled** | `47,398 ns/op` | `192 B/op` | `10 allocs/op` |
| `pool` Echo — **Unpooled** | `909,769 ns/op` | `1,382 B/op` | `40 allocs/op` |
| `rpc.Client.Call` (Pooled) | `77,338 ns/op` | `1,693 B/op` | `43 allocs/op` |

**Takeaway:** Connection pooling cuts loopback echo latency by **19.2x** and allocations by **4x** by eliminating per-request 3-way TCP handshakes and `TIME_WAIT` churn.

## Running

```bash
# Unit & failure-injection tests with race detector
go test -v -race ./00-networking/...

# Benchmarks
go test -bench=. -benchmem ./00-networking/...

# Live partial-failure demo
go run ./00-networking/cmd
```
