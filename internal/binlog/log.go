package binlog

import (
	"errors"
	"io"
)

var (
	ErrUnexpectedSize = errors.New("unexpected size data")

	ErrInvalidLog = errors.New("invalid log")
)

type LogID struct {
	Offset int64
	Size   int
}

type Log interface {
	Sign()
	Verify() bool

	Size() int

	Encode(dest io.Writer) (written int, err error)
	Decode(src io.Reader) (read int, err error)
}

type LogPool[L Log] interface {
	Alloc() (L, error)
	Free(L)
}

type LogBuilder[L Log] struct {
	newLog func() L
}

func NewLogBuilder[L Log](newLog func() L) LogBuilder[L] {
	return LogBuilder[L]{newLog: newLog}
}

func (lb LogBuilder[L]) Alloc() (L, error) {
	return lb.newLog(), nil
}

func (lb LogBuilder[L]) Free(_ L) {}
