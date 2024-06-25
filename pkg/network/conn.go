package network

import (
	"net"
	"sync/atomic"
	"time"
)

type Conn struct {
	net.Conn
	IdleTimeout time.Duration
	isClosed    atomic.Bool
}

func NewConn(stdConn net.Conn, idleTimeout time.Duration) *Conn {
	conn := &Conn{
		Conn:        stdConn,
		IdleTimeout: idleTimeout,
	}
	conn.updateDeadline()
	return conn
}

func (c *Conn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.updateDeadline()
	return n, err
}

func (c *Conn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.updateDeadline()
	return n, err
}

func (c *Conn) Close() error {
	if c.isClosed.CompareAndSwap(false, true) {
		return c.Conn.Close()
	}
	return nil
}

func (c *Conn) updateDeadline() {
	deadline := time.Now().Add(c.IdleTimeout)
	_ = c.SetDeadline(deadline)
}
