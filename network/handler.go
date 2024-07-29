package network

var NopHandler = func(conn *Conn) {}

type Handler func(conn *Conn)
