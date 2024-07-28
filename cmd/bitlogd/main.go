package main

import (
	"bufio"
	"bytes"
	"flag"
	"log"
	"net"

	"github.com/protomem/bitlog/bitcask"
	"github.com/protomem/bitlog/proto"
)

var (
	_path = flag.String("path", "./data", "path to store data")
	_addr = flag.String("addr", ":8080", "address to listen")
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

	lis, err := net.Listen("tcp", *_addr)
	if err != nil {
		log.Panicf("failed to listen: %v", err)
	}

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("failed to accept: %v", err)
			continue
		}

		log.Printf("accepted: %s", conn.RemoteAddr().String())

		go func(conn net.Conn) {
			defer conn.Close()
			defer log.Printf("closed: %s", conn.RemoteAddr().String())

			r := bufio.NewReader(conn)
			w := bufio.NewWriter(conn)
			s := bufio.NewScanner(r)

			for s.Scan() {
				if err := s.Err(); err != nil {
					log.Printf("failed to scan: %v", err)
					proto.Error(w, err.Error())
					w.Flush()
					break
				}

				req := s.Bytes()
				reqParts := bytes.Split(req, []byte{' '})

				var cmd proto.Command
				cmd.UnmarshalText(reqParts[0])
				if cmd == proto.UNKNOWN {
					log.Printf("unknown command: %s", string(reqParts[0]))
					proto.Error(w, "Unknown command")
					w.Flush()
					continue
				}

				switch cmd {
				case proto.PING:
					proto.Pong(w)
					w.Flush()
				case proto.GET:
					if len(reqParts) != 2 {
						proto.Error(w, "Not enough arguments")
						w.Flush()
						continue
					}
					key := reqParts[1]
					value, err := db.Get(key)
					if err != nil {
						if err == bitcask.ErrKeyNotFound {
							proto.Null(w)
							w.Flush()
							continue
						}

						proto.Error(w, err.Error())
						w.Flush()
						continue
					}
					proto.BulkString(w, string(value))
					w.Flush()
				case proto.SET:
					if len(reqParts) != 3 {
						proto.Error(w, "Not enough arguments")
						w.Flush()
						continue
					}
					key := reqParts[1]
					value := reqParts[2]
					if err := db.Put(key, value); err != nil {
						proto.Error(w, err.Error())
						w.Flush()
						continue
					}
					proto.OK(w)
					w.Flush()
				case proto.DEL:
					if len(reqParts) != 2 {
						proto.Error(w, "Not enough arguments")
						w.Flush()
						continue
					}
					key := reqParts[1]
					if err := db.Delete(key); err != nil {
						proto.Error(w, err.Error())
						w.Flush()
						continue
					}
					proto.Int(w, 1)
					w.Flush()
				}

			}
		}(conn)
	}
}
