package logging

import (
	"log"
	"log/slog"
	"os"
	"sync"
)

type Level = slog.Level

const (
	Debug = slog.LevelDebug
	Info  = slog.LevelInfo
	Warn  = slog.LevelWarn
	Error = slog.LevelError
)

var (
	_once sync.Once

	_debug *log.Logger
	_info  *log.Logger
	_warn  *log.Logger
	_error *log.Logger
)

func init() {
	_once.Do(func() {
		out := os.Stdout
		flags := log.Ldate | log.Ltime | log.Lmsgprefix

		_debug = log.New(out, "[DEBUG] ", flags)
		_info = log.New(out, "[INFO] ", flags)
		_warn = log.New(out, "[WARN] ", flags)
		_error = log.New(out, "[ERROR] ", flags|log.Lshortfile)
	})
}

func System(level Level) *log.Logger {
	return selectLogger(level)
}

func selectLogger(level Level) *log.Logger {
	switch level {
	case Debug:
		return _debug
	case Info:
		return _info
	case Warn:
		return _warn
	case Error:
		return _error
	default:
		return _debug
	}
}
