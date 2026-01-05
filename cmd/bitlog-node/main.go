package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const _appName = "bitlog-node"

var (
	_listenAddr = flag.String("addr", ":3957", "Listen address")
)

func main() {
	flag.Parse()
	log.SetPrefix(fmt.Sprintf("[%s] ", _appName))

	listener, err := net.Listen("tcp", *_listenAddr)
	if err != nil {
		log.Panicf("Failed to listen on %s: %v", *_listenAddr, err)
	}

	log.Printf("Listening on %s ...", *_listenAddr)

	var runnerGroup sync.WaitGroup
	runnerGroup.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					break
				}

				log.Printf("Failed to accept connection: %v", err)
				continue
			}

			runnerGroup.Go(func() {
				handleConnection(conn)
			})
		}
	})

	<-waitExit()

	log.Printf("Shutdown initiated ...")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var shutdownGroup sync.WaitGroup
	shutdownGroup.Go(func() {
		if err := listener.Close(); err != nil {
			log.Printf("Failed close listener: %v", err)
		}
	})

	shutdownDone := make(chan struct{})
	go func() {
		shutdownGroup.Wait()
		log.Printf("Shutdown operations done")

		runnerGroup.Wait()
		log.Printf("Runner group done")

		close(shutdownDone)
	}()

	select {
	case <-ctxShutdown.Done():
		log.Printf("Shutdown signal received")
	case <-shutdownDone:
		log.Printf("Shutdown completed")
	}
}

func handleConnection(conn net.Conn) {
	log.Printf("Accepted connection from %s", conn.RemoteAddr())
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Failed to read from connection: %v", err)
			return
		}

		log.Printf("Received %d bytes from %s", n, conn.RemoteAddr())
	}
}

func waitExit() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch
}
