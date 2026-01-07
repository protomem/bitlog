package database

import (
	"errors"
	"slices"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

type DB struct {
	opts options

	idx  *Index
	jrnl *Journal
}

func New(opts ...Option) (*DB, error) {
	appliedOpts, err := applyOptions(opts...)
	if err != nil {
		return nil, err
	}

	db := &DB{
		opts: appliedOpts,
		idx:  NewIndex(),
		jrnl: NewJournal(),
	}

	return db, nil
}

func (db *DB) Has(key []byte) error {
	_, ok := db.idx.Lookup(key)
	if !ok {
		return ErrKeyNotFound
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	entry, ok := db.idx.Lookup(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	record, ok, err := db.jrnl.Find(entry.Ref)
	if err != nil {
		return nil, err
	}
	if !ok || record.OpCode != OperationPut {
		return nil, ErrKeyNotFound
	}

	return slices.Clone(record.Value), nil
}

func (db *DB) Put(key []byte, value []byte) error {
	record := Record{
		Timestamp: time.Now().Unix(),
		OpCode:    OperationPut,
		Key:       slices.Clone(key),
		Value:     slices.Clone(value),
	}

	ref, err := db.jrnl.Write(record)
	if err != nil {
		return err
	}

	entry := Entry{
		Key: slices.Clone(key),
		Ref: ref,
	}

	db.idx.Insert(entry)

	return nil
}

func (db *DB) Delete(key []byte) error {
	record := Record{
		Timestamp: time.Now().Unix(),
		OpCode:    OperationDelete,
		Key:       slices.Clone(key),
	}

	_, err := db.jrnl.Write(record)
	if err != nil {
		return err
	}

	db.idx.Remove(string(key))

	return nil
}

type Entry struct {
	Key []byte
	Ref Reference
}

type Reference struct {
	Address int64
}

type Index struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewIndex() *Index {
	return &Index{
		entries: make(map[string]Entry),
	}
}

func (idx *Index) Lookup(key []byte) (Entry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entry, ok := idx.entries[string(key)]
	return entry, ok
}

func (idx *Index) Insert(entry Entry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[string(entry.Key)] = entry
}

func (idx *Index) Remove(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.entries, key)
}

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

type Journal struct {
	mu      sync.RWMutex
	records []Record
}

func NewJournal() *Journal {
	return &Journal{}
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
