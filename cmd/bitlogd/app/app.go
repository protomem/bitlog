package app

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"os"

	"github.com/protomem/bitlog/internal/apprunner"
	"github.com/protomem/bitlog/internal/binlog"
	"github.com/protomem/bitlog/internal/buffer"
	"github.com/protomem/bitlog/internal/network/tcp"
	"github.com/protomem/bitlog/internal/protokey"
	"github.com/protomem/bitlog/pkg/werrors"
)

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

	if rootPath := app.cfg.RootPath; rootPath != "" {
		driver, err := os.OpenFile(rootPath, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			log.Printf("failed open data file by error=%s", err)
			return
		}

		app.kvLog = binlog.NewFacade(driver)
	}

	if err := app.kvLog.Recover(); err != nil {
		log.Printf("failed recover key/values with error=%s", err)
		return
	}

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

	parser := protokey.NewParser(reader)
	respWriter := protokey.NewWriter(writer)
	router := app.newProtokeyRouter()

	for {
		command, err := parser.ReadCommand()
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				return
			case protokey.IsProtocolError(err):
				_ = respWriter.Error("ERR " + err.Error())
				_ = respWriter.Flush()
				return
			default:
				log.Printf("failed read command with error=%s", err)
				return
			}
		}

		if err := router.Serve(command, respWriter); err != nil {
			log.Printf("failed serve command with error=%s", err)
			return
		}

		if err := respWriter.Flush(); err != nil {
			log.Printf("failed flush response with error=%s", err)
			return
		}
	}
}

func (app *App) newProtokeyRouter() *protokey.Router {
	router := protokey.NewRouter()

	router.Handle("PING", func(args [][]byte, writer *protokey.Writer) error {
		switch len(args) {
		case 0:
			return writer.SimpleString("PONG")
		case 1:
			return writer.BulkString(args[0])
		default:
			return writer.Error("ERR wrong number of arguments for 'ping' command")
		}
	})

	router.Handle("SET", func(args [][]byte, writer *protokey.Writer) error {
		if len(args) != 2 {
			return writer.Error("ERR wrong number of arguments for 'set' command")
		}

		if err := app.kvLog.Set(args[0], args[1]); err != nil {
			log.Printf("failed set key=%q with error=%s", string(args[0]), err)
			return writer.Error("ERR internal error")
		}

		return writer.SimpleString("OK")
	})

	router.Handle("GET", func(args [][]byte, writer *protokey.Writer) error {
		if len(args) != 1 {
			return writer.Error("ERR wrong number of arguments for 'get' command")
		}

		value, exists, err := app.kvLog.Get(args[0])
		if err != nil {
			log.Printf("failed get key=%q with error=%s", string(args[0]), err)
			return writer.Error("ERR internal error")
		}
		if !exists {
			return writer.NullBulkString()
		}

		return writer.BulkString(value)
	})

	router.Handle("DEL", func(args [][]byte, writer *protokey.Writer) error {
		if len(args) == 0 {
			return writer.Error("ERR wrong number of arguments for 'del' command")
		}

		var deleted int64
		for _, key := range args {
			_, exists, err := app.kvLog.Get(key)
			if err != nil {
				log.Printf("failed check key=%q with error=%s", string(key), err)
				return writer.Error("ERR internal error")
			}
			if !exists {
				continue
			}

			if err = app.kvLog.Delete(key); err != nil {
				log.Printf("failed del key=%q with error=%s", string(key), err)
				return writer.Error("ERR internal error")
			}

			deleted++
		}

		return writer.Integer(deleted)
	})

	return router
}
