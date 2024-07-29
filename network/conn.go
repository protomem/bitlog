package network

import (
	"net"
	"sync/atomic"
	"time"
)

type Conn struct {
	net.Conn

	IdleTimeout time.Duration
	closed      atomic.Bool
}

func NewConn(stdConn net.Conn, idleTimeout time.Duration) *Conn {
	conn := &Conn{
		Conn:        stdConn,
		IdleTimeout: idleTimeout,
	}
	conn.updateDeadline()
	return conn
}

func (c *Conn) Write(p []byte) (int, error) {
	c.updateDeadline()
	return c.Conn.Write(p)
}

func (c *Conn) Read(b []byte) (int, error) {
	c.updateDeadline()
	return c.Conn.Read(b)
}

func (c *Conn) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	return c.Conn.Close()
}

func (c *Conn) updateDeadline() {
	idleDeadline := time.Now().Add(c.IdleTimeout)
	c.SetDeadline(idleDeadline)
}
