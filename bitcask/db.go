package bitcask

import (
	"errors"
	"time"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongSize   = errors.New("wrong size key or value")
)

type DB struct {
	memtable *MemTable
	sstable  *SSTable
}

func Open(path string) (*DB, error) {
	memtable := NewMemTable()

	sstable, err := NewSSTable(path)
	if err != nil {
		return nil, err
	}

	if err := sstable.LoadAllFiles(); err != nil {
		return nil, err
	}

	db := &DB{
		memtable: memtable,
		sstable:  sstable,
	}

	if err := db.indexing(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Keys() ([][]byte, error) {
	return db.memtable.Keys(), nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	idx, ok := db.memtable.Find(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	file := db.sstable.Get(idx.File)
	if file == nil {
		return nil, ErrFileNotFound
	}

	blob, err := file.Read(idx.Value)
	if err != nil {
		return nil, err
	}

	if !blob.Verify() {
		return nil, ErrInvalidValue
	}

	if blob.IsGrave() || blob.IsExpired() {
		db.memtable.Remove(key)
		return nil, ErrKeyNotFound
	}

	return blob.Value, nil
}

func (db *DB) Set(key, value []byte, expiration time.Duration) error {
	if len(key) == 0 || len(value) == 0 {
		return ErrWrongSize
	}

	var (
		now time.Time = time.Now()
		exp time.Time
	)

	if expiration != 0 {
		exp = now.Add(expiration)
	}

	blob := NewBlob(now, exp, key, value)
	file := db.sstable.GetActive()

	cursor, err := file.Write(blob)
	if err != nil {
		return err
	}

	idx := NewIndex(file.ID(), now, key, cursor)
	db.memtable.Insert(idx)

	return nil
}

func (db *DB) Delete(key []byte) error {
	now := time.Now()

	blob := NewBlobGrave(now, key)
	file := db.sstable.GetActive()

	if _, err := file.Write(blob); err != nil {
		return err
	}

	db.memtable.Remove(key)

	return nil
}

func (db *DB) Close() error {
	db.memtable.Clear()
	return db.sstable.Close()
}

func (db *DB) indexing() error {
	// TODO: Implement
	return nil
}
