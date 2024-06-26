package network

import (
	"errors"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	opts   ServerOptions
	lis    net.Listener
	handl  Handler
	track  *connsTracker
	isDone atomic.Bool
}

func NewServer(opts ServerOptions) (*Server, error) {
	lis, err := net.Listen("tcp", opts.Addr())
	if err != nil {
		return nil, err
	}

	return &Server{
		opts:  opts,
		lis:   lis,
		handl: nopHandler,
		track: newConnsTracker(),
	}, nil
}

func (s *Server) SetHandler(h Handler) {
	if h == nil {
		return
	}
	s.handl = h
}

func (s *Server) Serve() {
	defer func() {
		if err := s.Close(); err != nil {
			log.Printf("network/server: failed close: %v", err)
		}
	}()

	for !s.isDone.Load() {
		stdConn, err := s.lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Printf("network/server: failed accept: %v", err)
			continue
		}

		log.Printf("network/server: accept: %s", stdConn.RemoteAddr())

		conn := NewConn(stdConn, s.opts.IdleTimeout)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() error {
	if s.isDone.CompareAndSwap(false, true) {
		err := s.lis.Close()

		conns := s.track.all()
		for _, conn := range conns {
			if cerr := conn.Close(); cerr != nil {
				err = errors.Join(err, cerr)
			}
		}

		return err
	}
	return nil
}

func (s *Server) handleConn(conn *Conn) {
	s.track.pin(conn)
	defer func() {
		s.track.unpin(conn)
		if err := conn.Close(); err != nil {
			log.Printf("network/server: failed close conn(%s): %v", conn.RemoteAddr(), err)
		}
	}()
	s.handl.Handle(conn)
}

type ServerOptions struct {
	Host        string
	Port        int
	IdleTimeout time.Duration
}

func (opts ServerOptions) Addr() string {
	return net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))
}

type connsTracker struct {
	mux   sync.Mutex
	conns map[string]*Conn
}

func newConnsTracker() *connsTracker {
	return &connsTracker{
		conns: make(map[string]*Conn),
	}
}

func (t *connsTracker) pin(conn *Conn) {
	t.mux.Lock()
	t.add(conn)
	t.mux.Unlock()
}

func (t *connsTracker) unpin(conn *Conn) {
	t.mux.Lock()
	t.delete(conn)
	t.mux.Unlock()
}

func (t *connsTracker) init() {
	if t.conns == nil {
		t.conns = make(map[string]*Conn)
	}
}

func (t *connsTracker) add(conn *Conn) {
	t.init()
	t.conns[conn.RemoteAddr().String()] = conn
}

func (t *connsTracker) delete(conn *Conn) {
	delete(t.conns, conn.RemoteAddr().String())
}

func (t *connsTracker) all() []*Conn {
	t.mux.Lock()
	defer t.mux.Unlock()
	conns := make([]*Conn, 0, len(t.conns))
	for _, conn := range t.conns {
		conns = append(conns, conn)
	}
	return conns
}
