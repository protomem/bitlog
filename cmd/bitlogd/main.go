package main

import (
	"bufio"
	"flag"
	"log"
	"net"
	"strings"

	"github.com/protomem/bitlog/bitcask"
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
					w.Write([]byte("ERROR\r\n"))
					w.Flush()
					break
				}

				req := s.Text()
				reqParts := strings.Split(req, " ")

				switch reqParts[0] {
				case "GET":
					if len(reqParts) != 2 {
						log.Printf("invalid request: %s", req)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					key := reqParts[1]
					value, err := db.Get([]byte(key))
					if err != nil {
						if err == bitcask.ErrKeyNotFound {
							w.Write([]byte("NOT_FOUND\r\n"))
							w.Flush()
							continue
						}

						log.Printf("failed to get: %v", err)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					w.Write([]byte("VALUE " + key + " " + string(value) + "\r\n"))
					w.Flush()
				case "SET":
					if len(reqParts) != 3 {
						log.Printf("invalid request: %s", req)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					key := reqParts[1]
					value := reqParts[2]

					if err := db.Put([]byte(key), []byte(value)); err != nil {
						log.Printf("failed to put: %v", err)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					w.Write([]byte("STORED\r\n"))
					w.Flush()
				case "DEL":
					if len(reqParts) != 2 {
						log.Printf("invalid request: %s", req)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					key := reqParts[1]

					if err := db.Delete([]byte(key)); err != nil {
						log.Printf("failed to delete: %v", err)
						w.Write([]byte("ERROR\r\n"))
						w.Flush()
						continue
					}

					w.Write([]byte("DELETED\r\n"))
					w.Flush()
				default:
					log.Printf("invalid request: %s", req)
					w.Write([]byte("ERROR\r\n"))
					w.Flush()
				}
			}
		}(conn)
	}
}
