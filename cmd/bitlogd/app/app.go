package bitlogd

import "github.com/protomem/bitlog/pkg/syslog"

type App struct{}

func New() *App {
	return &App{}
}

func (app *App) Run() error {
	syslog.Infof("bitlogd version %s", "0.0.1")
	syslog.Infof("bitlogd run ...")

	return nil
}
