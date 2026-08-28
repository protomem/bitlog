package main

import (
	"github.com/protomem/bitlog/cmd/bitlogd/app"
)

func main() {
	var cfg app.Config
	cfg.LoadFromFlags()

	app.New(cfg).Run()
}
