package network

import (
	"errors"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ServerOptions struct {
	Addr string

	IdleTimeout time.Duration
}

type Server struct {
	opts ServerOptions
	lis  net.Listener

	h     Handler
	conns *connRegistry

	closed atomic.Bool
}

func NewServer(opts ServerOptions) *Server {
	return &Server{
		opts:  opts,
		h:     NopHandler,
		conns: newConnRegistry(),
	}
}

func (s *Server) SetHandler(h Handler) {
	if h == nil {
		return
	}
	s.h = h
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return err
	}
	s.lis = lis

	for !s.closed.Load() {
		stdConn, err := s.lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			log.Printf("tcp server: failed to accept: %v", err)
			continue
		}

		log.Printf("tcp server: accepted: %s", stdConn.RemoteAddr().String())

		conn := NewConn(stdConn, s.opts.IdleTimeout)
		go s.handleConn(conn)
	}

	return nil
}

func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	err := s.lis.Close()

	conns := s.conns.getAll()
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			log.Printf("tcp server: failed to close conn(%s): %v", conn.RemoteAddr(), err)
		}
	}

	return err
}

func (s *Server) handleConn(conn *Conn) {
	s.conns.add(conn)
	defer func() {
		s.conns.remove(conn)
		if err := conn.Close(); err != nil {
			log.Printf("tcp server: failed to close conn(%s): %v", conn.RemoteAddr(), err)
		}
	}()
	s.h(conn)
}

type connRegistry struct {
	mux   sync.Mutex
	conns map[string]*Conn
}

func newConnRegistry() *connRegistry {
	return &connRegistry{
		conns: make(map[string]*Conn),
	}
}

func (r *connRegistry) add(conn *Conn) {
	r.mux.Lock()
	defer r.mux.Unlock()

	r.conns[conn.RemoteAddr().String()] = conn
}

func (r *connRegistry) remove(conn *Conn) {
	r.mux.Lock()
	defer r.mux.Unlock()

	delete(r.conns, conn.RemoteAddr().String())
}

func (r *connRegistry) getAll() []*Conn {
	r.mux.Lock()
	defer r.mux.Unlock()

	conns := make([]*Conn, 0, len(r.conns))
	for _, conn := range r.conns {
		conns = append(conns, conn)
	}

	return conns
}
