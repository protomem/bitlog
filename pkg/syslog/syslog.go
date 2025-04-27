package syslog

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
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
		out := os.Stderr

		_debug = newLogger(out, DebugLevel)
		_info = newLogger(out, InfoLevel)
		_warn = newLogger(out, WarnLevel)
		_error = newLogger(out, ErrorLevel)
	})
}

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func Debug(v ...any) {
	getLog(DebugLevel).Print(v...)
}

func Debugf(format string, v ...any) {
	getLog(DebugLevel).Printf(format, v...)
}

func Info(v ...any) {
	getLog(InfoLevel).Print(v...)
}

func Infof(format string, v ...any) {
	getLog(InfoLevel).Printf(format, v...)
}

func Warn(v ...any) {
	getLog(WarnLevel).Print(v...)
}

func Warnf(format string, v ...any) {
	getLog(WarnLevel).Printf(format, v...)
}

func Error(v ...any) {
	getLog(ErrorLevel).Print(v...)
}

func Errorf(format string, v ...any) {
	getLog(ErrorLevel).Printf(format, v...)
}

func newLogger(out io.Writer, lvl Level) *log.Logger {
	if out == nil {
		out = io.Discard
	}

	flags := log.Ldate | log.Ltime | log.LUTC | log.Lmsgprefix
	prefix := fmtPrefix(lvl)

	return log.New(out, prefix, flags)
}

func fmtPrefix(lvl Level) string {
	return fmt.Sprintf("[%s] ", lvl.String())
}

func getLog(lvl Level) *log.Logger {
	switch lvl {
	case DebugLevel:
		return _debug
	case InfoLevel:
		return _info
	case WarnLevel:
		return _warn
	case ErrorLevel:
		return _error
	default:
		return _debug
	}
}
