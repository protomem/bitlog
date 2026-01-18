package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"

	"github.com/protomem/bitlog/internal/database"
	"github.com/protomem/bitlog/internal/network"
	"github.com/protomem/bitlog/internal/redisproto"
)

type Handler struct {
	db *database.DB
}

func NewHandler(db *database.DB) *Handler {
	return &Handler{
		db: db,
	}
}

func (h *Handler) ServeTcp(conn network.TcpConn) {
	log.Printf("Accepted connection from %s", conn.RemoteAddr())
	defer func() {
		conn.Close()
		log.Printf("Closed connection from %s", conn.RemoteAddr())
	}()

	for !conn.IsClosed() {
		reader := bufio.NewReader(conn)
		scanner := bufio.NewScanner(reader)

		for scanner.Scan() {
			buf := bytes.NewBuffer(scanner.Bytes())
			cmd, err := redisproto.CommandFromReader(buf)
			if err != nil {
				log.Printf("Failed to parse command: %v", err)
				continue
			}

			log.Printf("Received command: %s", cmd)

			switch cmd.Op {
			case redisproto.OpSet:
				if len(cmd.Args) != 2 {
					log.Printf("Invalid SET command: %s", cmd)
					continue
				}

				key := cmd.Args[0]
				value := cmd.Args[1]

				err := h.db.Put(key, value)
				if err != nil {
					log.Printf("Failed to set key: %v", err)
				}

			case redisproto.OpGet:
				if len(cmd.Args) != 1 {
					log.Printf("Invalid GET command: %s", cmd)
					continue
				}

				key := cmd.Args[0]
				value, err := h.db.Get(key)

				if err != nil {
					log.Printf("Failed to get key: %v", err)
				}
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)

			case redisproto.OpDel:
				if len(cmd.Args) != 1 {
					log.Printf("Invalid DEL command: %s", cmd)
					continue
				}

				key := cmd.Args[0]
				err := h.db.Delete(key)

				if err != nil {
					log.Printf("Failed to delete key: %v", err)
				}
				fmt.Fprintf(conn, "+OK\r\n")

			default:
				log.Printf("Unsupported command: %s", cmd.Op)
			}
		}
	}
}
