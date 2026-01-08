package database

import (
	"errors"
	"slices"
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

	wal := NewWriteAheadLog(nil, nil)

	db := &DB{
		opts: appliedOpts,
		idx:  NewIndex(),
		jrnl: NewJournal(wal),
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
