package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/protomem/bitlog/internal/database"
	"github.com/protomem/bitlog/internal/network"
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

	db, err := database.New(database.WithRootPath(*_dbPath))
	if err != nil {
		log.Panicf("Failed to initialize database: %v", err)
	}

	handler := NewHandler(db)

	srv := network.NewTcpServer()
	srv.SetHandler(handler)

	go func() {
		log.Printf("Starting server on %s ...", *_listenAddr)

		if err := srv.ListenAndServe(*_listenAddr); err != nil && !errors.Is(err, network.ErrServerClosed) {
			log.Panicf("Failed to start server: %v", err)
		}
	}()

	<-waitSysExit()

	log.Printf("Shutdown initiated ...")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), _shutdownTimeout)
	defer cancel()

	var shutdownGroup sync.WaitGroup

	shutdownGroup.Go(func() {
		if err := srv.Shutdown(ctxShutdown); err != nil {
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
