package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/protomem/bitlog/internal/bitcask"
	"github.com/protomem/bitlog/internal/network"
	"github.com/protomem/bitlog/internal/redisproto"
)

var (
	_listenAddr = flag.String("laddr", ":1337", "listen address")
	_dbPath     = flag.String("db", "", "path to bitcask database")
)

func init() {
	flag.Parse()
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db, err := bitcask.Open(*_dbPath)
	if err != nil {
		logger.Error("Failed to open database", "error", err)
		return
	}

	tcpsrv := network.NewTCPServer(logger)
	go func() {
		logger.Info("Listening ...", "address", *_listenAddr)

		if err := tcpsrv.Serve(*_listenAddr, network.TCPHandlerFunc(func(conn *network.TCPConn) {
			scanner := bufio.NewScanner(conn)
			reader := bufio.NewReader(conn)

			for scanner.Scan() {
				if err := scanner.Err(); err != nil {
					logger.Error("Failed to read bytes", "error", err)
					return
				}

				cmd, err := redisproto.NewCommandFromReader(reader)
				if err != nil {
					logger.Error("Failed to read command", "error", err)
					return
				}

				// Use database ...
				_ = db

				logger.Debug("Received command", "command", cmd)
			}

			time.Sleep(60 * time.Second)
		})); err != nil {
			logger.Error("Failed to serve", "error", err)
		}
	}()

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Interrupt)
	<-quitCh

	var errs error

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := tcpsrv.Shutdown(ctx); err != nil {
		errs = errors.Join(errs, err)
	}

	if err := db.Close(); err != nil {
		errs = errors.Join(errs, err)
	}

	if errs != nil {
		logger.Error("Failed to shutdown", "error", errs)
	} else {
		logger.Info("Shutdown complete")
	}
}
