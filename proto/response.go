package proto

import (
	"bytes"
	"io"
	"strconv"
)

const (
	_newline = "\r\n"

	_defaultPreffixSep = " "
	_missingPreffixSep = ""
	_splitPreffixSep   = _newline

	_stringMark     = "+"
	_intMark        = ":"
	_bulkStringMark = "$"
	_errorMark      = "-"
	_arrayMark      = "*"
)

func OK(w io.Writer) (int, error) {
	res := buildString(_stringMark, "OK", _missingPreffixSep)
	return w.Write(res.Bytes())
}

func Null(w io.Writer) (int, error) {
	res := buildString(_bulkStringMark, "-1", _missingPreffixSep)
	return w.Write(res.Bytes())
}

func Int(w io.Writer, value int) (int, error) {
	res := buildString(_intMark, strconv.Itoa(value), _missingPreffixSep)
	return w.Write(res.Bytes())
}

func Error(w io.Writer, err error) (int, error) {
	res := buildString(_errorMark, "ERR", _defaultPreffixSep, err.Error())
	return w.Write(res.Bytes())
}

func Pong(w io.Writer, msg ...string) (int, error) {
	res := buildString(_stringMark, "PONG", _defaultPreffixSep, msg...)
	return w.Write(res.Bytes())
}

func BulkString(w io.Writer, msg string) (int, error) {
	res := buildString(_bulkStringMark, strconv.Itoa(len(msg)), _newline, msg)
	return w.Write(res.Bytes())
}

// Array contain only strings
func Array(w io.Writer, msgs ...string) (int, error) {
	res := new(bytes.Buffer)

	// write header
	res.WriteString(_arrayMark)
	res.WriteString(strconv.Itoa(len(msgs)))
	res.WriteString(_splitPreffixSep)

	// write body
	for _, msg := range msgs {
		BulkString(res, msg)
	}

	return w.Write(res.Bytes())
}

func buildString(mark string, preffix string, preffixSep string, msg ...string) *bytes.Buffer {
	ss := new(bytes.Buffer)

	ss.WriteString(mark)
	ss.WriteString(preffix)
	ss.WriteString(preffixSep)
	if len(msg) != 0 {
		ss.WriteString(msg[0])
	}
	ss.WriteString(_newline)

	return ss
}
