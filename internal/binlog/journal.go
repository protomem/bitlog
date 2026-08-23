package binlog

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const _journalErrorMsg = "binlog/journal"

var ErrInvalidLog = errors.New("invalid log")

type Driver interface {
	io.WriterAt
	io.ReaderAt
}

type Journal[L Log] struct {
	driver Driver

	writeLock sync.Mutex
	headOff   int64
}

func NewJournal[L Log](driver Driver) *Journal[L] {
	if driver == nil {
		werrors.PanicMessage(_journalErrorMsg, "driver is nil")
	}

	return &Journal[L]{
		driver: driver,
	}
}

func (j *Journal[L]) Write(log L) (LogID, error) {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	log.Sign()

	lastOff := j.headOff
	rawBuf := make([]byte, log.Size())

	buf := bytes.NewBuffer(rawBuf)
	if _, err := log.Encode(buf); err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write", "log decode")
	}

	written, err := j.driver.WriteAt(buf.Bytes(), lastOff)
	if err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write")
	}

	j.headOff += int64(written)

	return LogID{Offset: lastOff, Size: written}, nil
}

func (j *Journal[L]) Read(lid LogID) (L, error) {
	var log L
	rawBuf := make([]byte, lid.Size)

	if _, err := j.driver.ReadAt(rawBuf, lid.Offset); err != nil {
		return log, werrors.Error(err, _journalErrorMsg, "read")
	}

	buf := bytes.NewBuffer(rawBuf)
	if _, err := log.Decode(buf); err != nil {
		return log, werrors.Error(err, _journalErrorMsg, "read", "log decode")
	}

	if !log.Verify() {
		return log, werrors.Error(ErrInvalidLog, _journalErrorMsg, "read", "verify")
	}

	return log, nil
}
