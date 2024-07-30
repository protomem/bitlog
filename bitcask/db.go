package bitcask

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
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
	basePath string

	index *keyDir

	mux   sync.RWMutex
	files map[int]*dataFile
}

func Open(path string) (*DB, error) {
	path = filepath.Clean(path)

	if err := createDirIfNotExists(path); err != nil {
		return nil, err
	}

	active, err := createDataFile(path)
	if err != nil {
		return nil, err
	}

	db := &DB{
		basePath: path,
		index:    newKeyDir(),
		files:    map[int]*dataFile{},
	}

	db.files[active.id] = active
	db.files[0] = active

	if err := db.openOlderFiles(); err != nil {
		return nil, err
	}

	if err := db.indexing(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	db.mux.Lock()
	defer db.mux.Unlock()

	var errs error

	for _, f := range db.files {
		if err := f.close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}

	return errs
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

	file := db.file(rec.fid)
	data, err := file.read(rec.offset, rec.size)
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

	file := db.activeFile()
	rec := newDataRecord(key, value)

	offset, written, err := file.append(rec)
	if err != nil {
		return err
	}

	db.index.insert(idxRecord{
		fid:    db.activeFile().id,
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

	file := db.activeFile()
	rec := newDataGrave(key)

	db.index.delete(key)

	_, _, err := file.append(rec)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) openOlderFiles() error {
	db.mux.Lock()
	defer db.mux.Unlock()

	dirEntries, err := os.ReadDir(db.basePath)
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}

		if db.files[0].f.Name() == entry.Name() {
			continue
		}

		file, err := openDataFile(filepath.Join(db.basePath, entry.Name()))
		if err != nil {
			return err
		}

		db.files[file.id] = file
	}

	return nil
}

func (db *DB) indexing() error {
	db.mux.Lock()
	defer db.mux.Unlock()

	files := make([]*dataFile, 0, len(db.files))
	for _, f := range db.files {
		files = append(files, f)
	}

	slices.SortFunc(files, func(a, b *dataFile) int {
		return int(a.tstamp - b.tstamp)
	})

	for _, f := range files {
		if err := f.foreach(func(data dataRecord, offset int64, size int) error {
			if data.isGrave() {
				db.index.delete(data.key)
				return nil
			}

			db.index.insert(idxRecord{
				fid:    f.id,
				key:    data.key,
				tstamp: data.tstamp,
				offset: offset,
				size:   size,
			})

			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) file(id int) *dataFile {
	db.mux.RLock()
	defer db.mux.RUnlock()
	return db.files[id]
}

func (db *DB) activeFile() *dataFile {
	return db.file(0)
}

func createDirIfNotExists(path string) error {
	return os.MkdirAll(path, 0o755)
}
