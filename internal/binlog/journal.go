package binlog

import (
	"io"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const _journalErrorMsg = "binlog/journal"

type Driver interface {
	io.WriterAt
	io.ReaderAt
}

type Journal struct {
	driver Driver

	writeLock sync.Mutex
	headOff   int64
}

func NewJournal(driver Driver) *Journal {
	if driver == nil {
		werrors.PanicMessage(_journalErrorMsg, "driver is nil")
	}

	return &Journal{
		driver: driver,
	}
}

func (j *Journal) Write(log KeyValueLog) (LogID, error) {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	log.Sign()

	var (
		data   = log.Encode()
		curOff = j.headOff
	)

	written, err := j.driver.WriteAt(data, curOff)
	if err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write")
	}

	j.headOff += int64(written)

	return LogID{Offset: curOff, Size: written}, nil
}

func (j *Journal) Read(lid LogID) (KeyValueLog, error) {
	data := make([]byte, lid.Size)
	_, err := j.driver.ReadAt(data, lid.Offset)
	if err != nil {
		return KeyValueLog{}, werrors.Error(err, _journalErrorMsg, "read")
	}

	var log KeyValueLog
	err = log.Decode(data)
	if err != nil {
		return KeyValueLog{}, werrors.Error(err, _journalErrorMsg, "decode")
	}

	if !log.Verify() {
		return KeyValueLog{}, werrors.Error(ErrInvalidLog, _journalErrorMsg, "verify")
	}

	return log, nil
}
