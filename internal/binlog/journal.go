package binlog

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/protomem/bitlog/pkg/ptr"
	"github.com/protomem/bitlog/pkg/werrors"
)

const _journalErrorMsg = "binlog/journal"

type Driver interface {
	io.WriterAt

	io.Reader
	io.ReaderAt
}

type Journal[L Log] struct {
	driver Driver
	lpool  LogPool[L]

	writeLock sync.Mutex
	headOff   int64
}

func NewJournal[L Log](driver Driver, lpool LogPool[L]) *Journal[L] {
	if driver == nil {
		werrors.PanicMessage(_journalErrorMsg, "driver is nil")
	}
	if lpool == nil {
		werrors.PanicMessage(_journalErrorMsg, "log pool is nil")
	}

	return &Journal[L]{
		driver: driver,
		lpool:  lpool,
	}
}

func (j *Journal[L]) Write(log L) (LogID, error) {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	log.Sign()

	lastOff := j.headOff
	rawBuf := make([]byte, 0, log.Size())

	buf := bytes.NewBuffer(rawBuf)
	if _, err := log.Encode(buf); err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write", "log encode")
	}

	written, err := j.driver.WriteAt(buf.Bytes(), lastOff)
	if err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write")
	}

	j.headOff += int64(written)

	return LogID{Offset: lastOff, Size: written}, nil
}

func (j *Journal[L]) Read(lid LogID) (L, error) {
	log, err := j.lpool.Alloc()
	if err != nil {
		return ptr.Zero[L](), werrors.Error(err, _journalErrorMsg, "log alloc")
	}

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

func (j *Journal[L]) Iter() *JournalIterator[L] {
	return NewJournalIterator(j.driver, j.lpool)
}

type JournalIterator[L Log] struct {
	driver Driver
	lpool  LogPool[L]

	lock    sync.RWMutex
	headOff int64
	lastErr error

	currLid LogID
	currLog L
}

func NewJournalIterator[L Log](driver Driver, lpool LogPool[L]) *JournalIterator[L] {
	return &JournalIterator[L]{
		driver: driver,
		lpool:  lpool,
	}
}

func (i *JournalIterator[L]) Err() error {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if errors.Is(i.lastErr, io.EOF) {
		return nil
	}

	return werrors.Error(i.lastErr, _journalErrorMsg, "iter")
}

func (i *JournalIterator[L]) Value() (LogID, L) {
	i.lock.RLock()
	defer i.lock.RUnlock()

	if i.lastErr != nil {
		return LogID{}, ptr.Zero[L]()
	}

	return i.currLid, i.currLog
}

func (i *JournalIterator[L]) Next() bool {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.lastErr != nil {
		return false
	}

	i.currLog, i.lastErr = i.lpool.Alloc()
	if i.lastErr != nil {
		return false
	}

	var decoded int
	decoded, i.lastErr = i.currLog.Decode(i.driver)
	if i.lastErr != nil {
		return false
	}

	i.currLid = LogID{
		Offset: i.headOff,
		Size:   i.currLog.Size(),
	}

	i.headOff += int64(decoded)

	return true
}
