package bitcask

import (
	"errors"
	"sync"
	"time"

	"github.com/protomem/bitlog/pkg/werrors"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrWrongSize   = errors.New("wrong size key or value")
)

type DB struct {
	// Lock for close and indexing
	gmux sync.RWMutex

	keydir   *IndexState
	registry *FileRegistry
}

func Open(path string) (*DB, error) {
	werr := werrors.Wrap("bitcask/open")

	keydir := NewIndexState()

	registry, err := NewFileRegistry(path)
	if err != nil {
		return nil, werr(err)
	}

	if err := registry.LoadAllFiles(); err != nil {
		return nil, werr(err)
	}

	db := &DB{
		keydir:   keydir,
		registry: registry,
	}

	if err := db.indexing(); err != nil {
		return nil, werr(err)
	}

	return db, nil
}

func (db *DB) Keys() ([][]byte, error) {
	db.gmux.RLock()
	defer db.gmux.RUnlock()

	return db.keydir.Keys(), nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.gmux.RLock()
	defer db.gmux.RUnlock()

	werr := werrors.Wrap("bitcask/get")

	idx, ok := db.keydir.Find(key)
	if !ok {
		return nil, werr(ErrKeyNotFound)
	}

	file := db.registry.Get(idx.File)
	if file == nil {
		return nil, werr(ErrFileNotFound)
	}

	entry, err := file.Read(idx.Cursor)
	if err != nil {
		return nil, werr(err)
	}

	if !entry.IsVerify() {
		return nil, werr(ErrKeyNotFound, "invalid entry")
	}

	if entry.IsTombstone() || entry.IsExpired() {
		db.keydir.Remove(key)
		return nil, werr(ErrKeyNotFound, "key expired or deleted")
	}

	return entry.Value, nil
}

func (db *DB) Set(key, value []byte, expiration time.Duration) error {
	db.gmux.RLock()
	defer db.gmux.RUnlock()

	werr := werrors.Wrap("bitcask/set")

	if len(key) == 0 || len(value) == 0 {
		return werr(ErrWrongSize)
	}

	var (
		now time.Time = time.Now()
		exp time.Time
	)

	if expiration != 0 {
		exp = now.Add(expiration)
	}

	entry := NewDataEntry(now.UnixMilli(), exp.UnixMilli(), key, value)
	file := db.registry.GetActive()

	cursor, err := file.Write(entry)
	if err != nil {
		return werr(err)
	}

	idx := NewIndexEntry(file.ID(), now.UnixMilli(), key, cursor)
	db.keydir.Insert(idx)

	return nil
}

func (db *DB) Delete(key []byte) error {
	db.gmux.RLock()
	defer db.gmux.RUnlock()

	werr := werrors.Wrap("bitcask/delete")
	now := time.Now()

	entry := NewTombstone(now.UnixMilli(), key)
	file := db.registry.GetActive()

	if _, err := file.Write(entry); err != nil {
		return werr(err)
	}

	db.keydir.Remove(key)

	return nil
}

func (db *DB) Close() error {
	db.gmux.Lock()
	defer db.gmux.Unlock()

	db.keydir.Clear()
	return db.registry.Close()
}

func (db *DB) indexing() error {
	db.gmux.Lock()
	defer db.gmux.Unlock()

	// TODO: Implement

	return werrors.Error(nil, "indexing")
}
