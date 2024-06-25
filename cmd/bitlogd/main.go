package main

import (
	"bufio"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/protomem/bitlog/pkg/network"
	"github.com/protomem/bitlog/pkg/version"
)

func main() {
	log.Printf("bitlogd: version '%s'", version.Get())

	opts := network.ServerOptions{
		Host:        "0.0.0.0",
		Port:        1337,
		IdleTimeout: 5 * time.Minute,
	}

	srv, err := network.NewServer(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := srv.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	srv.SetHandler(network.HandlerFunc(func(conn *network.Conn) {
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		s := bufio.NewScanner(r)

		for s.Scan() {
			if err := s.Err(); err != nil {
				log.Printf("bitlogd: failed read request %v", err)

				if errors.Is(err, os.ErrDeadlineExceeded) {
					return
				}

				continue
			}

			log.Printf("bitlogd: incoming request - %s", s.Text())

			_, _ = w.WriteString(strings.ToUpper(s.Text()) + "\r\n")

			if err := w.Flush(); err != nil {
				log.Printf("bitlogd: failed write response %v", err)
			}
		}
	}))

	log.Printf("bitlogd: listening on %s", opts.Addr())
	go srv.Serve()
	<-wait()
}

func wait() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	return ch
}
