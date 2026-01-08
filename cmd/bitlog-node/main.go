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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/protomem/bitlog/internal/database"
)

const (
	_appName = "bitlog-node"

	_shutdownTimeout = 15 * time.Second
)

var (
	_listenAddr = flag.String("addr", ":3957", "Listen address")
	_dbPath     = flag.String("db", "tmp/bitlog.db", "Database path")
)

func main() {
	flag.Parse()
	log.SetPrefix(fmt.Sprintf("[%s] ", _appName))

	db, err := database.New(
		database.WithRootPath(*_dbPath),
	)
	if err != nil {
		log.Panicf("Failed to initialize database: %v", err)
	}
	handler := NewHandler(db)

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
				handler.Handle(conn)
			})
		}
	})

	<-waitSysExit()

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

	shutdownGroup.Go(func() {
		if err := db.Close(); err != nil {
			log.Printf("Failed close database: %v", err)
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

func waitSysExit() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	return ch
}
