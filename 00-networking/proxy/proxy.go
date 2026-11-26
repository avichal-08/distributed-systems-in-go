package proxy

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"
)

type FaultConfig struct {
	Latency         time.Duration // base delay added to forwarded chunks
	Jitter          time.Duration // +/- random variance around Latency
	BandwidthBps    int64         // bytes/sec cap; 0 disables throttling
	Blackhole       bool          // accept client bytes, never forward to target
	ResetAfterBytes int64         // send tcp rst after forwarding N bytes; 0 disables
	ResetOnRequest  bool          // false = reset on server->client response (default)
}

type Proxy struct {
	listenAddr string
	targetAddr string
	logger     *slog.Logger

	mu     sync.RWMutex
	fault  FaultConfig
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool

	done chan struct{}
	wg   sync.WaitGroup
}

func New(listenAddr, targetAddr string, cfg FaultConfig, logger *slog.Logger) *Proxy {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Proxy{
		listenAddr: listenAddr,
		targetAddr: targetAddr,
		logger:     logger,
		fault:      cfg,
		conns:      make(map[net.Conn]struct{}),
		done:       make(chan struct{}),
	}
}

func (p *Proxy) Start() error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()

	p.logger.Info("fault proxy listening", "listen", ln.Addr().String(), "target", p.targetAddr)

	p.wg.Add(1)
	go p.acceptLoop(ln)
	return nil
}

func (p *Proxy) Addr() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.ln == nil {
		return ""
	}
	return p.ln.Addr().String()
}

// SetFault updates fault rules atomically for new and active streams.
func (p *Proxy) SetFault(cfg FaultConfig) {
	p.mu.Lock()
	p.fault = cfg
	p.mu.Unlock()
	p.logger.Info("proxy fault updated",
		"latency", cfg.Latency,
		"jitter", cfg.Jitter,
		"bps", cfg.BandwidthBps,
		"blackhole", cfg.Blackhole,
		"reset_after_bytes", cfg.ResetAfterBytes,
	)
}

func (p *Proxy) getFault() FaultConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.fault
}

func (p *Proxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	ln := p.ln
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()

	var err error
	if ln != nil {
		err = ln.Close()
	}
	p.wg.Wait()
	return err
}

func (p *Proxy) acceptLoop(ln net.Listener) {
	defer p.wg.Done()
	for {
		clientConn, err := ln.Accept()
		if err != nil {
			return
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = clientConn.Close()
			return
		}
		p.conns[clientConn] = struct{}{}
		p.wg.Add(1)
		p.mu.Unlock()

		go p.handleConn(clientConn)
	}
}

func (p *Proxy) handleConn(clientConn net.Conn) {
	defer func() {
		p.untrack(clientConn)
		_ = clientConn.Close()
		p.wg.Done()
	}()

	if p.getFault().Blackhole {
		p.logger.Debug("blackholing connection", "remote", clientConn.RemoteAddr().String())
		// drain bytes silently until client times out or closes
		_, _ = io.Copy(io.Discard, clientConn)
		return
	}

	targetConn, err := net.DialTimeout("tcp", p.targetAddr, 5*time.Second)
	if err != nil {
		p.logger.Warn("dial target failed", "target", p.targetAddr, "err", err)
		return
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = targetConn.Close()
		return
	}
	p.conns[targetConn] = struct{}{}
	p.mu.Unlock()

	defer func() {
		p.untrack(targetConn)
		_ = targetConn.Close()
	}()

	var pipeWG sync.WaitGroup
	pipeWG.Add(2)

	// client -> target (request path)
	go func() {
		defer pipeWG.Done()
		p.pipe(targetConn, clientConn, true)
		closeWrite(targetConn)
	}()

	// target -> client (response path)
	go func() {
		defer pipeWG.Done()
		p.pipe(clientConn, targetConn, false)
		closeWrite(clientConn)
	}()

	pipeWG.Wait()
}

func (p *Proxy) pipe(dst, src net.Conn, isRequest bool) {
	buf := make([]byte, 4096)
	var forwarded int64

	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			cfg := p.getFault()

			if cfg.Blackhole && isRequest {
				// drop request bytes without forwarding
				if readErr != nil {
					return
				}
				continue
			}

			if !p.sleepDelay(cfg.Latency, cfg.Jitter) {
				return
			}

			chunk := buf[:n]

			// trigger tcp rst once byte threshold is reached on the configured direction
			shouldReset := cfg.ResetAfterBytes > 0 && (cfg.ResetOnRequest == isRequest)
			if shouldReset && forwarded+int64(len(chunk)) >= cfg.ResetAfterBytes {
				remain := cfg.ResetAfterBytes - forwarded
				if remain > 0 {
					_, _ = dst.Write(chunk[:remain])
				}
				p.logger.Debug("injecting tcp reset", "request_path", isRequest, "after_bytes", cfg.ResetAfterBytes)
				resetTCP(dst)
				resetTCP(src)
				return
			}

			if err := p.writeThrottled(dst, chunk, cfg.BandwidthBps); err != nil {
				return
			}
			forwarded += int64(n)
		}

		if readErr != nil {
			return
		}
	}
}

func (p *Proxy) writeThrottled(dst net.Conn, data []byte, bps int64) error {
	if bps <= 0 {
		_, err := dst.Write(data)
		return err
	}

	const step = 1024
	for len(data) > 0 {
		n := min(len(data), step)
		if _, err := dst.Write(data[:n]); err != nil {
			return err
		}
		wait := time.Duration(float64(n) / float64(bps) * float64(time.Second))
		if !p.sleep(wait) {
			return net.ErrClosed
		}
		data = data[n:]
	}
	return nil
}

func (p *Proxy) sleepDelay(base, jitter time.Duration) bool {
	if base <= 0 && jitter <= 0 {
		return true
	}
	d := base
	if jitter > 0 {
		delta := time.Duration(rand.Int64N(int64(jitter)*2+1)) - jitter
		d += delta
	}
	if d <= 0 {
		return true
	}
	return p.sleep(d)
}

func (p *Proxy) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-p.done:
		return false
	}
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

func closeWrite(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	}
}

// SetLinger(0) forces the kernel to emit a TCP RST instead of a graceful FIN.
func resetTCP(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}
