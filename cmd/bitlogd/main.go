package main

import (
	"bufio"
	"errors"
	"log"
	"strings"

	"github.com/protomem/bitlog/internal/network/tcp"
)

const _newLine = "\r\n"

func main() {
	echoSrv := tcp.Server{
		ListenAddr: ":7743",
		Handler: tcp.HandlerFunc(func(conn tcp.Conn) {
			reader := bufio.NewReader(conn)
			writer := bufio.NewWriter(conn)

			scanner := bufio.NewScanner(reader)

			for scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Printf("received message with error=%s", err)
					return
				}

				msg := scanner.Text()
				log.Printf("received message %q", msg)

				if strings.EqualFold(msg, "close") {
					if err := conn.Close(); err != nil {
						log.Printf("failed close conn with error=%s", err)
					}
				}

				writer.Write([]byte(strings.ToUpper(msg)))
				writer.Write([]byte(_newLine))

				if err := writer.Flush(); err != nil {
					log.Printf("send message with error=%s", err)
				}
			}

			if err := scanner.Err(); err != nil && !errors.Is(err, tcp.ErrConnClosed) {
				log.Printf("scann failed with error=%s", err)
			}
		}),
	}

	if err := echoSrv.ListenAndServe(); err != nil {
		log.Printf("failed start tcp server with error=%s", err)
	} else {
		log.Printf("closed server")
	}
}
