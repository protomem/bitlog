package main

import bitlogd "github.com/protomem/bitlog/cmd/bitlogd/app"

func main() {
	app := bitlogd.New()
	_ = app.Run()
}
