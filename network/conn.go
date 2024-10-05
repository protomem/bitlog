package network

import (
	"net"

	"github.com/protomem/bitlog/pkg/werrors"
)

type Conn struct {
	stdConn net.Conn
}

func NewConn(stdConn net.Conn) *Conn {
	return &Conn{
		stdConn: stdConn,
	}
}

func (conn *Conn) LocalAddr() net.Addr {
	return conn.stdConn.LocalAddr()
}

func (conn *Conn) RemoteAddr() net.Addr {
	return conn.stdConn.RemoteAddr()
}

func (conn *Conn) Read(b []byte) (int, error) {
	read, err := conn.stdConn.Read(b)
	return read, werrors.Error(err, "tcpConn/read")
}

func (conn *Conn) Write(b []byte) (int, error) {
	written, err := conn.stdConn.Write(b)
	return written, werrors.Error(err, "tcpConn/write")
}
