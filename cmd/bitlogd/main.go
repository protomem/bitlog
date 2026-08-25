package main

import (
	"bufio"
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/protomem/bitlog/internal/network/tcp"
	"github.com/protomem/bitlog/pkg/werrors"

	"golang.org/x/sync/errgroup"
)

const (
	_newLine = "\r\n"

	_shutdownTimeout = 15 * time.Second
)

var errInterruptedBySignal = errors.New("process interrupted by signal")

func main() {
	group, groupCtx := errgroup.WithContext(context.Background())

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
		}),
	}

	group.Go(func() error {
		if err := echoSrv.ListenAndServe(); err != nil {
			return werrors.Error(err, "echoServer", "listenAndServe")
		}
		return nil
	})

	group.Go(func() error {
		<-groupCtx.Done()
		log.Printf("shutdown initiated, stopping server ...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), _shutdownTimeout)
		defer cancel()

		if err := echoSrv.Shutdown(shutdownCtx); err != nil {
			return werrors.Error(err, "echoServer", "shutdown")
		}

		return nil
	})

	group.Go(func() error {
		exitSig := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
		waitExitCh := make(chan os.Signal, len(exitSig))
		signal.Notify(waitExitCh, exitSig...)

		select {
		case <-waitExitCh:
			return errInterruptedBySignal
		case <-groupCtx.Done():
			return nil
		}
	})

	if err := group.Wait(); err != nil {
		if errors.Is(err, errInterruptedBySignal) {
			log.Printf("shutting down")
		} else {
			log.Printf("terminating wity error=%s", err)
		}
	}

}
