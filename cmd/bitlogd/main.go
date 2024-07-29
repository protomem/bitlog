package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/protomem/bitlog/bitcask"
	"github.com/protomem/bitlog/network"
	"github.com/protomem/bitlog/proto"
)

var (
	_path = flag.String("path", "./data", "path to store data")
	_addr = flag.String("addr", ":1337", "address to listen")
)

func init() {
	flag.Parse()
}

func main() {
	log.Printf("bitlogd version %s", "0.0.1")

	db, err := bitcask.Open(*_path)
	if err != nil {
		log.Panicf("failed to open db: %v", err)
	}
	defer db.Close()

	log.Printf("db opened: %s", *_path)

	srv := network.NewServer(network.ServerOptions{
		Addr:        *_addr,
		IdleTimeout: 5 * time.Minute,
	})

	srv.SetHandler(func(conn *network.Conn) {
		defer conn.Close()

		w := bufio.NewWriter(conn)
		r := bufio.NewReader(conn)
		s := bufio.NewScanner(r)

		for s.Scan() {
			if err := s.Err(); err != nil {
				log.Printf("failed to read: %v", err)
				return
			}

			log.Printf("got request: %s", s.Text())

			req := s.Bytes()
			reqParts := bytes.Split(req, []byte{' '})
			if len(reqParts) == 0 {
				log.Printf("empty request")
				continue
			}

			log.Printf("got command: %s arguments: %s", reqParts[0], reqParts[1:])

			var cmd proto.Command
			cmd.UnmarshalText(reqParts[0])

			switch cmd {
			case proto.PING:
				proto.Pong(w)
			case proto.GET:
				if len(reqParts) != 2 {
					errMsg := newWrongNumberOfArgumentsError(string(reqParts[0]))
					log.Println(errMsg)
					proto.Error(w, errMsg)
					goto FLUSH
				}

				key := reqParts[1]
				val, err := db.Get(key)
				if err != nil {
					if errors.Is(err, bitcask.ErrKeyNotFound) {
						proto.Null(w)
					} else {
						log.Printf("failed to get: %v", err)
						proto.Error(w, "unexpected error")
					}
					goto FLUSH
				}

				proto.BulkString(w, string(val))
			case proto.SET:
				if len(reqParts) != 3 {
					errMsg := newWrongNumberOfArgumentsError(string(reqParts[0]))
					log.Println(errMsg)
					proto.Error(w, errMsg)
					goto FLUSH
				}

				key := reqParts[1]
				val := reqParts[2]

				if err := db.Put(key, val); err != nil {
					log.Printf("failed to put: %v", err)
					proto.Error(w, "unexpected error")
					goto FLUSH
				}

				proto.OK(w)
			case proto.DEL:
				if len(reqParts) != 2 {
					errMsg := newWrongNumberOfArgumentsError(string(reqParts[0]))
					log.Println(errMsg)
					proto.Error(w, errMsg)
					goto FLUSH
				}

				key := reqParts[1]
				if err := db.Delete(key); err != nil {
					log.Printf("failed to delete: %v", err)
					proto.Error(w, "unexpected error")
					goto FLUSH
				}

				proto.Int(w, 1)
			case proto.UNKNOWN:
				fallthrough
			default:
				errMsg := newUnknownCommandError(string(reqParts[0]), bytesToStrings(reqParts[1:]))
				log.Println(errMsg)
				proto.Error(w, errMsg)
			}

		FLUSH:
			if err := w.Flush(); err != nil {
				log.Printf("failed to write: %v", err)
				return
			}
		}
	})

	closeErrCh := make(chan error)
	go func() {
		<-quit()

		closeErrCh <- srv.Close()
	}()

	log.Printf("listening on %s", *_addr)
	defer log.Printf("stopped listening on %s", *_addr)

	if err := srv.Serve(); err != nil {
		log.Panicf("failed to serve: %v", err)
	}

	if err := <-closeErrCh; err != nil {
		log.Printf("failed to close: %v", err)
	}
}

func newWrongNumberOfArgumentsError(cmd string) string {
	return fmt.Sprintf("wrong number of arguments for '%s' command", cmd)
}

func newUnknownCommandError(cmd string, args []string) string {
	return fmt.Sprintf("unknown command '%s', with args beginning with: %v", cmd, stringsToString(args))
}

func bytesToStrings(b [][]byte) []string {
	s := make([]string, len(b))
	for i, v := range b {
		s[i] = string(v)
	}
	return s
}

func stringsToString(ss []string) string {
	return strings.Join(ss, ", ")
}

func quit() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch
}
