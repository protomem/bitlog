package main

import (
	"bufio"
	"errors"
	"flag"
	"os"
	"os/signal"
	"time"

	"github.com/protomem/bitlog/bitcask"
	"github.com/protomem/bitlog/logging"
	"github.com/protomem/bitlog/network"
	"github.com/protomem/bitlog/proto"
)

var (
	_listenAddr = flag.String("addr", ":1337", "the address to listen on for incoming connections")
	_dbPath     = flag.String("db", "db", "the path to folder contains db files")
)

func init() {
	flag.Parse()
}

func main() {
	logging.
		System(logging.Info).
		Printf("bitlogd version %s", "0.1.0")

	db, err := bitcask.Open(*_dbPath)
	if err != nil {
		logging.
			System(logging.Error).
			Panicf("failed to open database(%s): %v", *_dbPath, err)
	}
	defer db.Close()

	conf := network.ServerConfig{ListenAddr: *_listenAddr}
	srv, err := network.NewServer(conf)
	if err != nil {
		logging.
			System(logging.Error).
			Panicf("failed to initialize server(%s): %v", *_listenAddr, err)
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

			req := s.Bytes()
			cmd, args, err := proto.ParseCommand(req)
			if err != nil {
				logging.
					System(logging.Error).
					Printf("failed to parse request from connection(%s): %v", conn.RemoteAddr(), err)

				proto.Error(w, err)
				goto FLUSH
			}

			switch cmd {
			case proto.PING:
				proto.Pong(w, args...)
			case proto.KEYS:
				keys, _ := db.Keys()
				proto.Array(w, proto.Bytes2Strings(keys...)...)
			case proto.GET:
				key := args[0]

				value, err := db.Get([]byte(key))
				if err != nil {
					logging.
						System(logging.Error).
						Printf("failed to get key(%s) from db: %v", key, err)

					if errors.Is(err, bitcask.ErrKeyNotFound) {
						proto.Null(w)
					} else {
						proto.Error(w, errors.New("internal error"))
					}

					goto FLUSH
				}

				proto.BulkString(w, string(value))
			case proto.SET:
				key := args[0]
				value := args[1]

				var exp time.Duration
				if len(args) == 3 {
					var err error
					exp, err = time.ParseDuration(args[2])
					if err != nil {
						logging.
							System(logging.Error).
							Printf("failed to parse expiration")
						goto FLUSH
					}
				}

				logging.
					System(logging.Debug).
					Printf(
						"set key(%s) with value(%s) and expiration(%s) from connection(%s) ",
						key, value, exp.String(), conn.RemoteAddr(),
					)

				if err := db.Set([]byte(key), []byte(value), exp); err != nil {
					logging.
						System(logging.Error).
						Printf("failed to set key(%s) to db: %v", key, err)

					proto.Error(w, errors.New("internal error"))
					goto FLUSH
				}

				proto.OK(w)
			case proto.DEL:
				key := args[0]

				if err := db.Delete([]byte(key)); err != nil {
					logging.
						System(logging.Error).
						Printf("failed to delete key(%s) in db: %v", key, err)

					proto.Error(w, errors.New("internal error"))
					goto FLUSH
				}

				proto.Int(w, 1)
			}

		FLUSH:
			if err := w.Flush(); err != nil {
				logging.
					System(logging.Error).
					Printf("failed to write to connection(%s): %v", conn.RemoteAddr(), err)

				break
			}
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
