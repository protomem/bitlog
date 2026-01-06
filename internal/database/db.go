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

	idx *Index
	jrn *Journal
}

func New(opts ...Option) (*DB, error) {
	appliedOpts, err := applyOptions(opts...)
	if err != nil {
		return nil, err
	}

	db := &DB{
		opts: appliedOpts,
		idx:  NewIndex(),
		jrn:  NewJournal(),
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

	record, ok, err := db.jrn.Find(entry.Ref)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrKeyNotFound
	}

	return slices.Clone(record.Value), nil
}

func (db *DB) Put(key []byte, value []byte) error {
	record := Record{
		Timestamp: time.Now().Unix(),
		Key:       slices.Clone(key),
		Value:     slices.Clone(value),
	}

	ref, err := db.jrn.Append(record)
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
		Key:       slices.Clone(key),
	}

	ref, err := db.jrn.Append(record)
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

type operationCode int

const (
	_operationPut operationCode = iota
	_operationDelete
)

type Record struct {
	opcode operationCode

	Timestamp int64
	Key       []byte
	Value     []byte
}

type Journal struct {
	mu      sync.RWMutex
	records []Record
}

func NewJournal() *Journal {
	return &Journal{}
}

func (jrn *Journal) Find(ref Reference) (Record, bool, error) {
	jrn.mu.RLock()
	defer jrn.mu.RUnlock()

	if len(jrn.records) < int(ref.Address) {
		return Record{}, false, nil
	}

	record := jrn.records[ref.Address]
	if record.Key == nil || record.Value == nil {
		return Record{}, false, ErrCorruptedRecord
	}
	if record.opcode != _operationPut {
		return Record{}, false, nil
	}

	return record, true, nil
}

func (jrn *Journal) Append(record Record) (Reference, error) {
	record.opcode = _operationPut

	return jrn.addRecord(record)
}

func (jrn *Journal) Delete(record Record) (Reference, error) {
	record.opcode = _operationDelete
	record.Value = nil

	return jrn.addRecord(record)
}

func (jrn *Journal) addRecord(record Record) (Reference, error) {
	jrn.mu.Lock()
	defer jrn.mu.Unlock()

	jrn.records = append(jrn.records, record)
	address := int64(len(jrn.records) - 1)

	return Reference{Address: address}, nil
}
