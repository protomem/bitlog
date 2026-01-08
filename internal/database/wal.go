package database

import (
	"io"
	"sync"
)

type ReadDriver interface {
	io.ReaderAt
}

type WriteDriver interface {
	io.WriterAt
}

type WriteAheadLog struct {
	wd WriteDriver
	rd ReadDriver

	offsetMu   sync.Mutex
	lastOffset int64
}

func NewWriteAheadLog(wd WriteDriver, rd ReadDriver) *WriteAheadLog {
	return &WriteAheadLog{
		wd: wd,
		rd: rd,
	}
}

func (wal *WriteAheadLog) Read(offset int64, data []byte) (read int, err error) {
	read, err = wal.rd.ReadAt(data, offset)
	return
}

func (wal *WriteAheadLog) Write(data []byte) (offset int64, written int, err error) {
	offset = wal.addAndPrevOffset(int64(len(data)))
	written, err = wal.wd.WriteAt(data, offset)
	return
}

func (wal *WriteAheadLog) addAndPrevOffset(delta int64) int64 {
	wal.offsetMu.Lock()
	defer wal.offsetMu.Unlock()

	prevOffset := wal.lastOffset
	wal.lastOffset += delta

	return prevOffset
}
