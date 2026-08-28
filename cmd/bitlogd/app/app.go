package app

import (
	"bufio"
	"context"
	"errors"
	"log"
	"strings"

	"github.com/protomem/bitlog/internal/apprunner"
	"github.com/protomem/bitlog/internal/network/tcp"
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

	app.runner.ExitOnSystemSignal()
	if err := app.runner.WaitTerminating(); err != nil {
		switch {
		case errors.Is(err, apprunner.ErrInterruptedBySignal):
			log.Printf("shutting down")
		default:
			log.Printf("terminating wity error=%s", err)
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

		msg := scanner.Text()
		log.Printf("received message %q", msg)

		if strings.EqualFold(msg, "close") {
			return
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
}
