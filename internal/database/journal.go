package database

import (
	"errors"
	"sync"
)

var ErrCorruptedRecord = errors.New("corrupted record")

type OperationCode int

const (
	OperationPut OperationCode = iota
	OperationDelete
)

type Record struct {
	Timestamp int64
	OpCode    OperationCode

	Key   []byte
	Value []byte
}

type Reference struct {
	Address int64
}

type Journal struct {
	mu      sync.RWMutex
	records []Record
	wal     *WriteAheadLog
}

func NewJournal(wal *WriteAheadLog) *Journal {
	return &Journal{wal: wal}
}

func (jrnl *Journal) Find(ref Reference) (Record, bool, error) {
	jrnl.mu.RLock()
	defer jrnl.mu.RUnlock()

	if len(jrnl.records) < int(ref.Address) {
		return Record{}, false, nil
	}

	record := jrnl.records[ref.Address]
	if record.Key == nil || record.Value == nil {
		return Record{}, false, ErrCorruptedRecord
	}

	return record, true, nil
}

func (jrnl *Journal) Write(record Record) (Reference, error) {
	jrnl.mu.Lock()
	defer jrnl.mu.Unlock()

	jrnl.records = append(jrnl.records, record)
	address := int64(len(jrnl.records) - 1)

	return Reference{Address: address}, nil
}

func (jrnl *Journal) Flush() error {
	jrnl.mu.Lock()
	defer jrnl.mu.Unlock()

	return nil
}
