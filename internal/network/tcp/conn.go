package tcp

import (
	"io"
	"net"
	"sync"
)

var ErrConnClosed = net.ErrClosed

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
	defer c.Close()
	c.Server.Handler.ServeTCP(c)
}

func (c *serverConn) Read(b []byte) (int, error) {
	read, err := c.Conn.Read(b)
	return read, err
}

func (c *serverConn) Write(b []byte) (int, error) {
	written, err := c.Conn.Write(b)
	return written, err
}

func (c *serverConn) Close() error {
	c.onceClose.Do(c.close)
	c.Server.trackConn(c, false)

	return c.closeErr
}

func (c *serverConn) close() {
	c.closeErr = c.Conn.Close()
}
