package network

import (
	"net"
	"time"
)

type DialOptions struct {
	Addr string

	IdleTimeout time.Duration
}

func Dial(opts DialOptions) (*Conn, error) {
	stdConn, err := net.Dial("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	return NewConn(stdConn, opts.IdleTimeout), nil
}
