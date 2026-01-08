package database

import (
	"bytes"
	"encoding/gob"
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
	Size    int64
}

type Journal struct {
	mu  sync.RWMutex
	wal *WriteAheadLog
}

func NewJournal(wal *WriteAheadLog) *Journal {
	return &Journal{wal: wal}
}

func (jrnl *Journal) Find(ref Reference) (Record, error) {
	jrnl.mu.RLock()
	defer jrnl.mu.RUnlock()

	blob := make([]byte, ref.Size)
	if _, err := jrnl.wal.Read(ref.Address, blob); err != nil {
		return Record{}, err
	}

	buf := bytes.NewBuffer(blob)
	decoder := gob.NewDecoder(buf)

	var record Record
	if err := decoder.Decode(&record); err != nil {
		return Record{}, err
	}

	return record, nil
}

func (jrnl *Journal) Write(record Record) (Reference, error) {
	jrnl.mu.Lock()
	defer jrnl.mu.Unlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)

	if err := encoder.Encode(record); err != nil {
		return Reference{}, err
	}

	address, written, err := jrnl.wal.Write(buf.Bytes())
	if err != nil {
		return Reference{}, err
	}

	return Reference{Address: address, Size: int64(written)}, nil
}
