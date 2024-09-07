package network

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/protomem/bitlog/logging"
)

var NopHandler = HandlerFunc(func(*Conn) {})

type Handler interface {
	Handle(conn *Conn)
}

type HandlerFunc func(conn *Conn)

func (fn HandlerFunc) Handle(conn *Conn) {
	fn(conn)
}

type ServerConfig struct {
	ListenAddr string
}

type Server struct {
	conf ServerConfig
	lis  net.Listener
	h    Handler

	mux   sync.RWMutex
	conns map[*Conn]struct{}

	isClose atomic.Bool
}

func NewServer(conf ServerConfig) (*Server, error) {
	s := &Server{conf: conf}
	if err := s.initListener(); err != nil {
		return nil, err
	}

	s.SetHandler(NopHandler)

	return s, nil
}

func (s *Server) Addr() net.Addr {
	return s.lis.Addr()
}

func (s *Server) SetHandler(h Handler) {
	if h == nil {
		return
	}

	s.h = h
}

func (s *Server) Listen() error {
	defer func() {
		if err := s.closeListener(); err != nil {
			logging.
				System(logging.Error).
				Printf("failed to close listener: %v", err)
		}
	}()

	for !s.isClose.Load() {
		conn, err := s.lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}

			logging.
				System(logging.Warn).
				Printf("failed to accept connection: %v", err)

			continue
		}

		logging.
			System(logging.Debug).
			Printf("accepted connection from %s", conn.RemoteAddr())

		go s.handleConn(conn)
	}

	return nil
}

func (s *Server) Close() error {
	if s.isClose.Swap(true) {
		return nil
	}

	var errs error

	errs = errors.Join(errs, s.closeListener())

	conns := s.clearConns()
	for _, conn := range conns {
		if err := conn.stdConn.Close(); err != nil {
			logging.
				System(logging.Error).
				Printf("failed to close connection(%s): %v", conn.RemoteAddr(), err)

			errs = errors.Join(errs, err)
		} else {
			logging.
				System(logging.Debug).
				Printf("closed connection(%s)", conn.RemoteAddr())
		}
	}

	return errs
}

func (s *Server) initListener() error {
	var err error

	s.lis, err = net.Listen("tcp", s.conf.ListenAddr)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) closeListener() error {
	if err := s.lis.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	return nil
}

func (s *Server) handleConn(stdConn net.Conn) {
	conn := NewConn(stdConn)

	s.trackConn(conn)
	defer func() {
		if !s.removeConn(conn) {
			return
		}

		if err := conn.stdConn.Close(); err != nil {
			logging.
				System(logging.Error).
				Printf("failed to close connection(%s): %v", conn.RemoteAddr(), err)
		} else {
			logging.
				System(logging.Debug).
				Printf("closed connection(%s)", conn.RemoteAddr())
		}
	}()

	s.h.Handle(conn)
}

func (s *Server) trackConn(conn *Conn) {
	s.mux.Lock()
	defer s.mux.Unlock()

	if s.conns == nil {
		s.conns = make(map[*Conn]struct{})
	}

	s.conns[conn] = struct{}{}
}

func (s *Server) removeConn(conn *Conn) bool {
	s.mux.Lock()
	defer s.mux.Unlock()

	_, ok := s.conns[conn]
	delete(s.conns, conn)

	return ok
}

func (s *Server) clearConns() []*Conn {
	s.mux.Lock()
	defer s.mux.Unlock()

	conns := make([]*Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}

	s.conns = make(map[*Conn]struct{})

	return conns
}

func (s *Server) numConns() int {
	s.mux.RLock()
	defer s.mux.RUnlock()
	return len(s.conns)
}
