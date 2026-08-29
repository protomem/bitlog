package app

import (
	"flag"
	"time"
)

const (
	_defaultListenAddr         = ":7743"
	_defaultShutdownTimeout    = 15 * time.Second
	_defaultPreallocMemorySize = 4 * 1024 * 1024 // 4 Mb
)

type Config struct {
	ListenAddr         string
	ShutdownTimeout    time.Duration
	PreallocMemorySize int
}

func (cfg *Config) LoadFromFlags() {
	defer flag.Parse()

	flag.StringVar(&cfg.ListenAddr, "listen-addr", _defaultListenAddr, "listen address")
	flag.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", _defaultShutdownTimeout, "shutdown timeout")
	flag.IntVar(&cfg.PreallocMemorySize, "prealloc-memory-size", _defaultPreallocMemorySize, "preallocation memory size")
}
