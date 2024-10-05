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
	lock sync.RWMutex

	keydir *IndexState
	files  *FileRegistry
}

func Open(path string) (*DB, error) {
	werr := werrors.Wrap("bitcask/open")

	keydir := NewIndexState()

	files, err := NewFileRegistry(path)
	if err != nil {
		return nil, werr(err)
	}

	if err := files.LoadAllFiles(); err != nil {
		return nil, werr(err)
	}

	db := &DB{
		keydir: keydir,
		files:  files,
	}

	if err := db.indexing(); err != nil {
		return nil, werr(err)
	}

	return db, nil
}

func (db *DB) Keys() ([][]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.keydir.Keys(), nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	werr := werrors.Wrap("bitcask/get")

	idx, ok := db.keydir.Find(key)
	if !ok {
		return nil, werr(ErrKeyNotFound)
	}

	file := db.files.Get(idx.File)
	if file == nil {
		return nil, werr(ErrKeyNotFound, "file not found")
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

func (db *DB) Set(key, value []byte, dur time.Duration) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	werr := werrors.Wrap("bitcask/set")

	if len(key) == 0 || len(value) == 0 {
		return werr(ErrWrongSize)
	}

	now, exp := unixTimestampWithExpiration(dur)

	entry := NewDataEntry(now, exp, key, value)
	file := db.files.GetActive()

	cursor, err := file.Write(entry)
	if err != nil {
		return werr(err)
	}

	idx := NewIndexEntry(file.ID(), now, exp, key, cursor)
	db.keydir.Insert(idx)

	return nil
}

func (db *DB) Delete(key []byte) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	werr := werrors.Wrap("bitcask/delete")
	now := unixTimestamp()

	entry := NewTombstone(now, key)
	file := db.files.GetActive()

	if _, err := file.Write(entry); err != nil {
		return werr(err)
	}

	db.keydir.Remove(key)

	return nil
}

func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.keydir.Clear()
	return werrors.Error(db.files.Close(), "bitcask/close")
}

func (db *DB) indexingWithLock() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.indexing()
}

func (db *DB) indexing() error {
	var errs error

	db.files.Range(func(file *DataFile) {
		iter, err := NewDataFileIterator(file)
		if err != nil {
			errs = errors.Join(errs, err)
			return
		}

		for iter.Next() {
			entry, cur, err := iter.Result()
			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}

			if entry.IsTombstone() || entry.IsExpired() {
				if _, ok := db.keydir.Find(entry.Key); ok {
					db.keydir.Remove(entry.Key)
				}
			} else {
				idx := NewIndexEntry(file.ID(), entry.Created, entry.Expired, entry.Key, cur)
				db.keydir.Insert(idx)
			}
		}
	})

	return werrors.Error(errs, "indexing")
}
