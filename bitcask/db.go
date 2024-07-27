package bitcask

import (
	"errors"
)

var ErrKeyNotFound = errors.New("key not found")

type DB struct {
	index *keyDir
	file  *dataFile
}

func Open(path string) (*DB, error) {
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

func (db *DB) Get(key []byte) ([]byte, error) {
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
	rec := newDataRecord(key, value)

	offset, written, err := db.file.append(rec)
	if err != nil {
		return err
	}

	db.index.insert(keyDirRecord{
		fid:    db.file.id,
		key:    key,
		tstamp: rec.tstamp,
		offset: offset,
		size:   written,
	})

	return nil
}

func (db *DB) Delete(key []byte) error {
	rec := newDataGrave(key)

	_, _, err := db.file.append(rec)
	if err != nil {
		return err
	}

	db.index.delete(key)

	return nil
}
