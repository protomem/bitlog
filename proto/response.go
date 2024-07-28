package proto

import (
	"io"
	"strconv"
)

const (
	_term = "\r\n"

	_stringMarker = "+"
	_intMarker    = ":"
	_errorMarker  = "-"
	_bulkMarker   = "$"
	_arrayMarker  = "*"

	_errorPrefix = "ERR"
)

func String(w io.Writer, msg string) (int, error) {
	msgB := []byte(_stringMarker + msg + _term)
	return w.Write(msgB)
}

func OK(w io.Writer) (int, error) {
	return String(w, "OK")
}

func Pong(w io.Writer) (int, error) {
	return String(w, "PONG")
}

func Int(w io.Writer, val int64) (int, error) {
	msgB := []byte(_intMarker + strconv.FormatInt(val, 10) + _term)
	return w.Write(msgB)
}

func Error(w io.Writer, msg string) (int, error) {
	msgB := []byte(_errorMarker + _errorPrefix + " " + msg + _term)
	return w.Write(msgB)
}

func BulkString(w io.Writer, msg string) (int, error) {
	msgB := []byte(_bulkMarker + strconv.Itoa(len(msg)) + _term + msg + _term)
	return w.Write(msgB)
}

func Null(w io.Writer) (int, error) {
	msgB := []byte(_bulkMarker + "-1" + _term)
	return w.Write(msgB)
}

func Array(w io.Writer, msgs []string) (int, error) {
	header := []byte(_arrayMarker + strconv.Itoa(len(msgs)) + _term)
	written, err := w.Write(header)
	if err != nil {
		return written, err
	}

	for _, msg := range msgs {
		msgB := []byte(_bulkMarker + strconv.Itoa(len(msg)) + _term + msg + _term)
		subWritten, err := w.Write(msgB)
		written += subWritten
		if err != nil {
			return written, err
		}
	}

	return written, nil
}
