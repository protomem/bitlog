package apprunner

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
)

var ErrInterruptedBySignal = errors.New("process interrupted by signal")

type Runner struct {
	groupCtx context.Context
	group    *errgroup.Group
}

func New() *Runner {
	group, groupCtx := errgroup.WithContext(context.Background())

	return &Runner{
		groupCtx: groupCtx,
		group:    group,
	}
}

func (r *Runner) Run(runFn func(ctx context.Context) error) {
	r.group.Go(func() error {
		return runFn(r.groupCtx)
	})
}

func (r *Runner) ExitOnSystemSignal() {
	r.Run(func(ctx context.Context) error {
		exitSig := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
		waitExitCh := make(chan os.Signal, len(exitSig))
		signal.Notify(waitExitCh, exitSig...)

		select {
		case <-waitExitCh:
			return ErrInterruptedBySignal
		case <-r.groupCtx.Done():
			return nil
		}
	})
}

func (r *Runner) WaitTerminating() error {
	return r.group.Wait()
}
