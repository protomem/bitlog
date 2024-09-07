package network

import "net"

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
	return conn.stdConn.Read(b)
}

func (conn *Conn) Write(b []byte) (int, error) {
	return conn.stdConn.Write(b)
}
