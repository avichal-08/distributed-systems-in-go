package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/avichal-08/distributed-systems-in-go/00-networking/frame"
)

var ErrServerClosed = errors.New("rpc: server closed")

// AppError indicates the server executed the request and returned a domain error.
type AppError struct {
	Message string
}

func (e *AppError) Error() string { return "rpc app error: " + e.Message }

type HandlerFunc func(ctx context.Context, payload []byte) ([]byte, error)

type ServerOption func(*Server)

func WithServerCodec(c Codec) ServerOption        { return func(s *Server) { s.codec = c } }
func WithServerFramer(f frame.Framer) ServerOption { return func(s *Server) { s.framer = f } }
func WithServerLogger(l *slog.Logger) ServerOption { return func(s *Server) { s.logger = l } }

type Server struct {
	codec  Codec
	framer frame.Framer
	logger *slog.Logger

	mu       sync.RWMutex
	handlers map[string]HandlerFunc
	ln       net.Listener
	conns    map[net.Conn]struct{}
	closed   bool

	wg sync.WaitGroup
}

func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		codec:    JSONCodec{},
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		handlers: make(map[string]HandlerFunc),
		conns:    make(map[net.Conn]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) Register(method string, h HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrServerClosed
	}
	s.ln = ln
	s.mu.Unlock()

	s.logger.Info("rpc server listening", "addr", ln.Addr().String())

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			s.logger.Warn("accept error", "err", err)
			continue
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return ErrServerClosed
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	ln := s.ln
	// unblock idle conns sitting in ReadFrame while letting active handlers finish writing
	now := time.Now()
	for c := range s.conns {
		_ = c.SetReadDeadline(now)
	}
	s.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// force-close remaining connections if drain budget expired
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		_ = conn.Close()
		s.wg.Done()
	}()

	remote := conn.RemoteAddr().String()
	s.logger.Debug("conn accepted", "remote", remote)

	for {
		raw, err := s.framer.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !s.isClosed() {
				s.logger.Debug("conn read closed", "remote", remote, "err", err)
			}
			return
		}

		req, err := s.codec.DecodeRequest(raw)
		if err != nil {
			s.logger.Warn("malformed request", "remote", remote, "err", err)
			return
		}

		resp := s.dispatch(context.Background(), req)
		out, err := s.codec.EncodeResponse(resp)
		if err != nil {
			s.logger.Error("encode response failed", "id", req.ID, "err", err)
			return
		}

		if err := s.framer.WriteFrame(conn, out); err != nil {
			s.logger.Debug("write response failed", "remote", remote, "id", req.ID, "err", err)
			return
		}
	}
}

func (s *Server) dispatch(ctx context.Context, req Request) (resp Response) {
	resp.ID = req.ID

	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()

	if !ok {
		resp.Error = fmt.Sprintf("unknown method: %s", req.Method)
		return resp
	}

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("handler panic", "method", req.Method, "id", req.ID, "panic", r)
			resp.Payload = nil
			resp.Error = fmt.Sprintf("handler panic: %v", r)
		}
	}()

	payload, err := h(ctx, req.Payload)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}
	resp.Payload = payload
	return resp
}

func (s *Server) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}
