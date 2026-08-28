package app

import (
	"flag"
	"time"
)

const (
	_defaultListenAddr      = ":7743"
	_defaultShutdownTimeout = 15 * time.Second
)

type Config struct {
	ListenAddr      string
	ShutdownTimeout time.Duration
}

func (cfg *Config) LoadFromFlags() {
	defer flag.Parse()

	flag.StringVar(&cfg.ListenAddr, "listen-addr", _defaultListenAddr, "listen address")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", _defaultShutdownTimeout, "shutdown timeout")
}
