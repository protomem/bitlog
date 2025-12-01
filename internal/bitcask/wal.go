package bitcask

import (
	"bytes"
	"errors"
	"sync/atomic"
)

var ErrWriteAheadLogCorrupted = errors.New("write ahead log corrupted")

type AppendLog [][]byte

func (l AppendLog) Bytes() []byte {
	return bytes.Join(l, nil)
}

// TODO: With buffer logs
type WriteAheadLog struct {
	lastPos atomic.Int64

	wd WriteDriver
}

func NewWriteAheadLog(wd WriteDriver) *WriteAheadLog {
	return &WriteAheadLog{
		lastPos: atomic.Int64{},
		wd:      wd,
	}
}

func (wal *WriteAheadLog) Write(log AppendLog) (w int, pos int64, err error) {
	data := log.Bytes()

	nextPos := wal.lastPos.Add(int64(len(data)))
	pos = nextPos - int64(len(data))

	w, err = wal.wd.WriteAt(data, pos)
	if err != nil {
		return
	}
	if w != len(data) {
		err = ErrWriteAheadLogCorrupted
		return
	}

	pos = wal.lastPos.Add(int64(w))

	return
}
