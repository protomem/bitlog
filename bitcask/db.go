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
	keydir   *IndexState
	registry *FileRegistry
}

func Open(path string) (*DB, error) {
	keydir := NewIndexState()

	registry, err := NewFileRegistry(path)
	if err != nil {
		return nil, err
	}

	if err := registry.LoadAllFiles(); err != nil {
		return nil, err
	}

	db := &DB{
		keydir:   keydir,
		registry: registry,
	}

	if err := db.indexing(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Keys() ([][]byte, error) {
	return db.keydir.Keys(), nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	idx, ok := db.keydir.Find(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	file := db.registry.Get(idx.File)
	if file == nil {
		return nil, ErrFileNotFound
	}

	dentry, err := file.Read(idx.Cursor)
	if err != nil {
		return nil, err
	}

	if !dentry.Verify() {
		return nil, ErrInvalidValue
	}

	if dentry.IsTombstone() || dentry.IsExpired() {
		db.keydir.Remove(key)
		return nil, ErrKeyNotFound
	}

	return dentry.Value, nil
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

	dentry := NewDataEntry(now, exp, key, value)
	file := db.registry.GetActive()

	cursor, err := file.Write(dentry)
	if err != nil {
		return err
	}

	idx := NewIndexEntry(file.ID(), now, key, cursor)
	db.keydir.Insert(idx)

	return nil
}

func (db *DB) Delete(key []byte) error {
	now := time.Now()

	dentry := NewTombstone(now, key)
	file := db.registry.GetActive()

	if _, err := file.Write(dentry); err != nil {
		return err
	}

	db.keydir.Remove(key)

	return nil
}

func (db *DB) Close() error {
	db.keydir.Clear()
	return db.registry.Close()
}

func (db *DB) indexing() error {
	// TODO: Implement
	return nil
}
