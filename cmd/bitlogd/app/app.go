package app

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/protomem/bitlog/internal/apprunner"
	"github.com/protomem/bitlog/internal/binlog"
	"github.com/protomem/bitlog/internal/buffer"
	"github.com/protomem/bitlog/internal/network/tcp"
	"github.com/protomem/bitlog/internal/protokey"
	"github.com/protomem/bitlog/pkg/werrors"
)

const _newLine = "\r\n"

type App struct {
	cfg    Config
	runner *apprunner.Runner
	kvLog  *binlog.Facade
}

func New(cfg Config) *App {
	preallocBuf := make([]byte, 0, cfg.PreallocMemorySize)

	return &App{
		cfg:    cfg,
		runner: apprunner.New(),
		kvLog:  binlog.NewFacade(buffer.NewDynamic(preallocBuf)),
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

func (app *App) handleServeTCP(conn tcp.Conn) {
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
			fmt.Fprintf(writer, "INVALID COMMAND '%s'", err)
			log.Printf("parse received message with error=%s", err)
			goto FLUSH
		}

		log.Printf("parsed command %d with args %+v", cmd, args)

		switch cmd {
		default:
			writer.WriteString("UNSUPORTED COMMAND")

		case protokey.PING:
			writer.WriteString("PONG")

		case protokey.SET:
			key := args[protokey.KeyKind]
			value := args[protokey.ValueKind]

			if err := app.kvLog.Set(key, value); err != nil {
				fmt.Fprintf(writer, "INVALID EXEC COMMAND '%s'", err)
			}

			writer.WriteString("OK")

		case protokey.GET:
			key := args[protokey.KeyKind]

			value, keyExists, err := app.kvLog.Get(key)
			if err != nil {
				fmt.Fprintf(writer, "INVALID EXEC COMMAND '%s'", err)
			}
			if !keyExists {
				writer.WriteString("KEY NOT FOUND")
			}

			writer.Write(value)

		case protokey.DEL:
			key := args[protokey.KeyKind]

			if err := app.kvLog.Delete(key); err != nil {
				fmt.Fprintf(writer, "INVALID EXEC COMMAND '%s'", err)
			}

			writer.WriteString("OK")
		}

	FLUSH:
		writer.WriteString(_newLine)
		if err := writer.Flush(); err != nil {
			log.Printf("send message with error=%s", err)
		}
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, tcp.ErrConnClosed) {
		log.Printf("scann failed with error=%s", err)
	}
}
