package network

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const _maxBufferSize = 10 * 1024 * 1024 // 10MB

var (
	ErrClosed = errors.New("conn closed")

	ErrServerClosed = errors.New("server closed")
)

type TcpConn interface {
	io.ReadWriteCloser

	RemoteAddr() net.Addr

	IsClosed() bool
}

type TcpHandler interface {
	ServeTcp(conn TcpConn)
}

type TcpServer struct {
	mu sync.RWMutex

	inShutdown atomic.Bool

	h TcpHandler

	listenRuntime sync.WaitGroup
	listeners     map[net.Listener]struct{}

	connsRuntime sync.WaitGroup
	activeConns  map[TcpConn]struct{}
}

func NewTcpServer() *TcpServer {
	return &TcpServer{}
}

func (s *TcpServer) SetHandler(h TcpHandler) {
	if h == nil {
		return
	}

	s.h = h
}

func (s *TcpServer) ListenAndServe(addr string) error {
	if s.inShutdown.Load() {
		return ErrServerClosed
	}

	rawListener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	listener := &tcpListener{Listener: rawListener}
	defer listener.Close()

	if !s.trackListener(listener) {
		return ErrServerClosed
	}
	defer s.untrackListener(listener)

	for {
		rawConn, err := listener.Accept()
		if err != nil {
			if s.inShutdown.Load() {
				return ErrServerClosed
			}

			// TODO Handle net.Error

			return err
		}

		conn := newTcpConn(s, rawConn)
		s.pinConn(conn)

		s.connsRuntime.Go(func() {
			conn.serve()
		})
	}
}

func (s *TcpServer) Shutdown(ctx context.Context) error {
	s.inShutdown.Store(true)

	listenErr := s.stopListen()
	s.listenRuntime.Wait()

	closeErr := s.closeAllConns()
	waiterConns := s.waitStopingConns()

	shutdownPollIntervalMax := 500 * time.Millisecond
	pollIntervalBase := time.Millisecond
	nextPollInterval := func() time.Duration {
		// Add 10% jitter.
		interval := pollIntervalBase + time.Duration(rand.Intn(int(pollIntervalBase/10)))
		// Double and clamp for next time.
		pollIntervalBase *= 2
		if pollIntervalBase > shutdownPollIntervalMax {
			pollIntervalBase = shutdownPollIntervalMax
		}
		return interval
	}

	timer := time.NewTimer(nextPollInterval())
	defer timer.Stop()

	for {
		select {
		case <-waiterConns:
			var err error
			if listenErr != nil {
				err = errors.Join(err, listenErr)
			}
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return err

		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			timer.Reset(nextPollInterval())
		}
	}
}

func (s *TcpServer) stopListen() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs error
	for listener := range s.listeners {
		if err := listener.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (s *TcpServer) closeAllConns() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs error
	for conn := range s.activeConns {
		if err := conn.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (s *TcpServer) waitStopingConns() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		s.connsRuntime.Wait()
		ch <- struct{}{}
		close(ch)
	}()
	return ch
}

func (s *TcpServer) trackListener(listener net.Listener) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listeners == nil {
		s.listeners = make(map[net.Listener]struct{})
	}

	if s.inShutdown.Load() {
		return false
	}

	s.listeners[listener] = struct{}{}
	s.listenRuntime.Add(1)

	return true
}

func (s *TcpServer) untrackListener(listener net.Listener) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.listeners, listener)
	s.listenRuntime.Done()
}

func (s *TcpServer) pinConn(conn *tcpConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeConns == nil {
		s.activeConns = make(map[TcpConn]struct{})
	}

	s.activeConns[conn] = struct{}{}
}

func (s *TcpServer) unpinConn(conn *tcpConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.activeConns, conn)
}

type tcpListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (l *tcpListener) Close() error {
	l.once.Do(func() {
		l.closeErr = l.Listener.Close()
	})

	return l.closeErr
}

// TODO set idle timeout
// TODO max buffer size
// TODO check if conn/server is already closed
type tcpConn struct {
	once     sync.Once
	isClosed atomic.Bool
	closeErr error

	srv  *TcpServer
	conn net.Conn

	reader io.Reader
	writer io.Writer
}

func newTcpConn(srv *TcpServer, rawConn net.Conn) *tcpConn {
	conn := &tcpConn{
		srv:  srv,
		conn: rawConn,

		reader: io.LimitReader(rawConn, _maxBufferSize),
		writer: bufio.NewWriter(rawConn),
	}

	return conn
}

func (c *tcpConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *tcpConn) Read(p []byte) (n int, err error) {
	return c.reader.Read(p)
}

func (c *tcpConn) Write(p []byte) (n int, err error) {
	return c.conn.Write(p)
}

func (c *tcpConn) Close() error {
	c.once.Do(func() {
		c.isClosed.Store(true)
		c.closeErr = c.conn.Close()
	})

	return c.closeErr
}

func (c *tcpConn) IsClosed() bool {
	return c.isClosed.Load()
}

func (c *tcpConn) serve() {
	defer func() {
		// TODO Handle panic

		c.Close()
		c.srv.unpinConn(c)
	}()

	c.srv.h.ServeTcp(c)
}
