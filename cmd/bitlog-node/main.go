package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/protomem/bitlog/internal/redisproto"
)

const (
	_appName = "bitlog-node"

	_shutdownTimeout = 15 * time.Second
)

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

	var (
		runnerGroup sync.WaitGroup
		isRunning   atomic.Bool
	)

	runnerGroup.Go(func() {
		isRunning.Store(true)
		defer func() {
			isRunning.CompareAndSwap(true, false)
			log.Printf("Stop listening")
		}()

		log.Printf("Listening on %s ...", *_listenAddr)

		for isRunning.Load() {
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

	ctxShutdown, cancel := context.WithTimeout(context.Background(), _shutdownTimeout)
	defer cancel()

	var shutdownGroup sync.WaitGroup

	shutdownGroup.Go(func() {
		isRunning.Store(false)
		log.Printf("Runner group send shutdown signal")
	})

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

		shutdownDone <- struct{}{}
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

	for {
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
		}
	}
}

func waitExit() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch
}
