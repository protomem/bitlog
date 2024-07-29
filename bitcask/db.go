package bitcask

import (
	"errors"
	"os"
)

const (
	_minKeySize   = 1
	_minValueSize = 1
)

var (
	ErrKeyNotFound           = errors.New("key not found")
	ErrInvalidKeyOrValueSize = errors.New("invalid key/value size")
)

type DB struct {
	index *keyDir
	file  *dataFile
}

func Open(path string) (*DB, error) {
	if err := createDirIfNotExists(path); err != nil {
		return nil, err
	}

	f, err := createDataFile(path)
	if err != nil {
		return nil, err
	}

	return &DB{
		index: newKeyDir(),
		file:  f,
	}, nil
}

func (db *DB) Close() error {
	return db.file.close()
}

func (db *DB) Keys() ([][]byte, error) {
	return db.index.allKeys(), nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) < _minKeySize {
		return nil, ErrInvalidKeyOrValueSize
	}

	rec, ok := db.index.lookup(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	data, err := db.file.read(rec.offset, rec.size)
	if err != nil {
		return nil, err
	}

	if err := data.verify(); err != nil {
		return nil, err
	}

	if data.isGrave() {
		return nil, ErrKeyNotFound
	}

	return data.value, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) < _minKeySize || len(value) < _minValueSize {
		return ErrInvalidKeyOrValueSize
	}

	rec := newDataRecord(key, value)

	offset, written, err := db.file.append(rec)
	if err != nil {
		return err
	}

	db.index.insert(idxRecord{
		fid:    db.file.id,
		key:    key,
		tstamp: rec.tstamp,
		offset: offset,
		size:   written,
	})

	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) < _minKeySize {
		return ErrInvalidKeyOrValueSize
	}

	rec := newDataGrave(key)

	_, _, err := db.file.append(rec)
	if err != nil {
		return err
	}

	db.index.delete(key)

	return nil
}

func createDirIfNotExists(path string) error {
	return os.MkdirAll(path, 0o755)
}
