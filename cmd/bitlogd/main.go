package main

import (
	"bufio"
	"flag"
	"os"
	"os/signal"
	"strings"

	"github.com/protomem/bitlog/logging"
	"github.com/protomem/bitlog/network"
)

var _listenAddr = flag.String("addr", ":1337", "the address to listen on for incoming connections")

func main() {
	logging.
		System(logging.Info).
		Printf("bitlogd version %s", "0.1.0")

	conf := network.ServerConfig{ListenAddr: *_listenAddr}
	srv, err := network.NewServer(conf)
	if err != nil {
		logging.
			System(logging.Error).
			Panicf("failed to initialize server: %v", err)
	}

	srv.SetHandler(network.HandlerFunc(func(conn *network.Conn) {
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		s := bufio.NewScanner(r)

		for s.Scan() {
			if err := s.Err(); err != nil {
				logging.
					System(logging.Error).
					Printf("failed to read from connection(%s): %v", conn.RemoteAddr(), err)

				continue
			}

			req := s.Text()
			res := strings.ToTitle(req)

			logging.
				System(logging.Debug).
				Printf("read %d bytes from %s", len(req), conn.RemoteAddr())

			var written int
			written += ignoreError(w.WriteString(res))

			w.WriteString("\r\n") // NOTE: summarize CRLF with `written` ?

			if err := w.Flush(); err != nil {
				logging.
					System(logging.Error).
					Printf("failed to write to connection(%s): %v", conn.RemoteAddr(), err)

				break
			}

			logging.
				System(logging.Debug).
				Printf("write %d bytes to %s", written, conn.RemoteAddr())
		}
	}))

	closeErr := make(chan error, 1)
	go func() {
		quitCh := make(chan os.Signal, 1)
		signal.Notify(quitCh, os.Interrupt)
		<-quitCh

		closeErr <- srv.Close()
	}()

	logging.
		System(logging.Info).
		Printf("listening on %s", srv.Addr())

	if err := srv.Listen(); err != nil {
		logging.
			System(logging.Error).
			Printf("failed to listen on %s: %v", srv.Addr(), err)
	}

	if err := <-closeErr; err != nil {
		logging.
			System(logging.Error).
			Printf("failed to close server: %v", err)
	}
}

func ignoreError[T any](value T, _ error) T {
	return value
}
