package tcp

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/protomem/bitlog/pkg/werrors"
)

const (
	_serverErrorMsg = "tcp/server"

	_shutdownPollIntervalMax = 500 * time.Millisecond
)

var ErrServerClosed = errors.New("server closed")

type Handler interface {
	ServeTCP(conn Conn)
}

type HandlerFunc func(conn Conn)

func (fn HandlerFunc) ServeTCP(conn Conn) {
	fn(conn)
}

type Server struct {
	ListenAddr string
	Handler    Handler

	inShutdown atomic.Bool

	mu         sync.Mutex
	listeners  map[*net.Listener]struct{}
	activeConn map[*serverConn]struct{}

	listenerGroup sync.WaitGroup
}

func (s *Server) ListenAndServe() error {
	rawListener, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return werrors.Error(err, _serverErrorMsg, "listen")
	}

	var listener net.Listener = &onceCloseListener{Listener: rawListener}
	defer listener.Close()

	if !s.trackListener(&listener, true) {
		return ErrServerClosed
	}
	defer s.trackListener(&listener, false)

	for {
		rawConn, err := listener.Accept()
		if err != nil {
			if s.shuttingDown() {
				return werrors.Error(ErrServerClosed, _serverErrorMsg, "accept")
			}

			return werrors.Error(err, _serverErrorMsg, "accept")
		}

		conn := s.registerConn(rawConn)
		go conn.Serve()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.inShutdown.Store(true)

	lnerr := s.closeListenersLocked()
	s.listenerGroup.Wait()

	nextPollInterval := s.shutdownJitter(time.Millisecond)

	timer := time.NewTimer(nextPollInterval())
	defer timer.Stop()

	for {
		if s.closeIdleConns() {
			return werrors.Error(lnerr, _serverErrorMsg, "shutdown")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(nextPollInterval())
		}
	}
}

func (s *Server) Close() error {
	s.inShutdown.Store(true)

	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.closeListenersLocked()

	s.mu.Unlock()
	s.listenerGroup.Wait()
	s.mu.Lock()

	for conn := range s.activeConn {
		conn.Close()
		delete(s.activeConn, conn)
	}

	return werrors.Error(err, _serverErrorMsg, "close")
}

func (s *Server) trackListener(ln *net.Listener, add bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listeners == nil {
		s.listeners = make(map[*net.Listener]struct{})
	}
	if add {
		if s.shuttingDown() {
			return false
		}
		s.listeners[ln] = struct{}{}
		s.listenerGroup.Add(1)
	} else {
		delete(s.listeners, ln)
		s.listenerGroup.Done()
	}
	return true
}

func (s *Server) closeListenersLocked() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	for ln := range s.listeners {
		if cerr := (*ln).Close(); cerr != nil && err == nil {
			err = cerr
		}
	}

	return err
}

func (s *Server) closeIdleConns() bool {
	// TODO
	return true
}

func (s *Server) registerConn(rawConn net.Conn) *serverConn {
	conn := &serverConn{Server: s, Conn: rawConn}
	s.trackConn(conn, true)

	return conn
}

func (s *Server) trackConn(conn *serverConn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.activeConn == nil {
		s.activeConn = make(map[*serverConn]struct{})
	}

	if add {
		s.activeConn[conn] = struct{}{}
	} else {
		delete(s.activeConn, conn)
	}
}

func (s *Server) shuttingDown() bool {
	return s.inShutdown.Load()
}

func (s *Server) shutdownJitter(pollIntervalBase time.Duration) func() time.Duration {
	return func() time.Duration {
		// Add 10% jitter.
		interval := pollIntervalBase + time.Duration(rand.Intn(int(pollIntervalBase/10)))
		// Double and clamp for next time.
		pollIntervalBase *= 2
		if pollIntervalBase > _shutdownPollIntervalMax {
			pollIntervalBase = _shutdownPollIntervalMax
		}
		return interval
	}
}

type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(oc.close)
	return oc.closeErr
}

func (oc *onceCloseListener) close() { oc.closeErr = oc.Listener.Close() }
