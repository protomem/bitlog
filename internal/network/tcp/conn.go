package tcp

import "io"

type Conn interface {
	io.Reader
	io.Writer

	io.Closer
}

type serverConn struct{}

func (c *serverConn) Serve() {}

func (c *serverConn) Close() error {
	return nil
}
