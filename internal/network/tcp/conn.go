package tcp

import (
	"io"
	"net"
	"sync"
)

type Conn interface {
	io.Reader
	io.Writer

	io.Closer
}

type serverConn struct {
	net.Conn
	Server *Server

	onceClose sync.Once
	closeErr  error
}

func (c *serverConn) Serve() {
	c.Server.Handler.ServeTCP(c)
}

func (c *serverConn) Close() error {
	c.onceClose.Do(c.close)
	c.Server.trackConn(c, false)

	return c.closeErr
}

func (c *serverConn) close() {
	c.closeErr = c.Conn.Close()
}
