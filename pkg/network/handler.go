package network

type Handler interface {
	Handle(conn *Conn)
}

type HandlerFunc func(conn *Conn)

func (fn HandlerFunc) Handle(conn *Conn) {
	fn(conn)
}

var nopHandler Handler = HandlerFunc(func(_ *Conn) {})
