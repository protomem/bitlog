package network

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net"
	"sync"
	"sync/atomic"
)

type TCPConn struct {
	conn net.Conn
}

func NewTCPConn(conn net.Conn) *TCPConn {
	return &TCPConn{
		conn: conn,
	}
}

func (c *TCPConn) Read(b []byte) (int, error) {
	return c.conn.Read(b)
}

func (c *TCPConn) Close() error {
	return c.conn.Close()
}

type TCPHandler interface {
	Handle(conn *TCPConn)
}

type TCPHandlerFunc func(conn *TCPConn)

func (f TCPHandlerFunc) Handle(conn *TCPConn) {
	f(conn)
}

type TCPSever struct {
	logger *slog.Logger

	connsLock sync.Mutex
	conns     map[*TCPConn]struct{}

	wg         sync.WaitGroup
	isRunning  atomic.Bool
	isShutdown atomic.Bool
}

func NewTCPServer(logger *slog.Logger) *TCPSever {
	return &TCPSever{
		logger: logger,
		conns:  make(map[*TCPConn]struct{}),
	}
}

func (s *TCPSever) Serve(addr string, handler TCPHandler) error {
	if !s.isRunning.CompareAndSwap(false, true) {
		return errors.New("tcp sever running")
	}
	defer s.isRunning.Store(false)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	for s.isRunning.Load() && !s.isShutdown.Load() {
		// TODO move reader to goroutine
		rawConn, err := lis.Accept()
		if err != nil {
			s.logger.Error("Failed to accept connection", "error", err)
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()

			s.logger.Debug("Handle connection", "address", rawConn.RemoteAddr())

			conn := NewTCPConn(rawConn)

			s.connsLock.Lock()
			s.conns[conn] = struct{}{}
			s.connsLock.Unlock()

			handler.Handle(conn)
		}()
	}

	return nil
}

func (s *TCPSever) Shutdown(ctx context.Context) error {
	s.isShutdown.Store(true)
	defer s.isShutdown.Store(false)

	s.isRunning.Store(false)

	s.connsLock.Lock()
	conns := maps.Keys(maps.Clone(s.conns))
	s.connsLock.Unlock()

	for conn := range conns {
		if err := conn.Close(); err != nil {
			s.logger.Error("Failed to close connection", "error", err)
		}
	}

	wait := make(chan struct{})
	defer close(wait)

	go func() {
		s.wg.Wait()
		wait <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait:
	}

	return nil
}
