package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log"

	"github.com/protomem/bitlog/internal/apprunner"
	"github.com/protomem/bitlog/internal/network/tcp"
	"github.com/protomem/bitlog/internal/protokey"
	"github.com/protomem/bitlog/pkg/werrors"
)

const _newLine = "\r\n"

type App struct {
	cfg    Config
	runner *apprunner.Runner
}

func New(cfg Config) *App {
	return &App{
		cfg:    cfg,
		runner: apprunner.New(),
	}
}

func (app *App) Run() {
	log.Printf("app run with config=%+v", app.cfg)

	mainServer := tcp.Server{
		ListenAddr: app.cfg.ListenAddr,
		Handler:    tcp.HandlerFunc(app.handleServeTCP),
	}

	app.runner.Run(func(_ context.Context) error {
		if err := mainServer.ListenAndServe(); err != nil {
			return werrors.Error(err, "main server", "listenAndServe")
		}
		return nil
	})

	app.runner.Run(func(ctx context.Context) error {
		<-ctx.Done()
		log.Printf("shutdown initiated, stopping server ...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), app.cfg.ShutdownTimeout)
		defer cancel()

		if err := mainServer.Shutdown(shutdownCtx); err != nil {
			return werrors.Error(err, "main server", "shutdown")
		}

		return nil
	})

	app.runner.StopOnSystemSignal()
	if err := app.runner.WaitTerminating(); err != nil {
		switch {
		case errors.Is(err, apprunner.ErrInterruptedBySignal):
			log.Printf("shutting down")
		default:
			log.Printf("terminating with error=%s", err)
		}
	}
}

func (*App) handleServeTCP(conn tcp.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			log.Printf("received message with error=%s", err)
			return
		}

		buf := bytes.NewBuffer(scanner.Bytes())

		cmd, args, err := protokey.Parse(buf)
		if err != nil {
			log.Printf("parse received message with error=%s", err)
		}

		log.Printf("parsed command %d with args %+v", cmd, args)

		switch cmd {
		default:
			writer.WriteString("UNSUPORTED COMMAND")

		case protokey.PING:
			writer.WriteString("PONG")
		}

		writer.WriteString(_newLine)
		if err := writer.Flush(); err != nil {
			log.Printf("send message with error=%s", err)
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, tcp.ErrConnClosed) {
		log.Printf("scann failed with error=%s", err)
	}
}
