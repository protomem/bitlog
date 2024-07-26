package bitcask

import (
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	_dataFileExt = ".data"

	_minFileID = 100000
	_maxFileID = 999999
)

type dataFile struct {
	mux sync.RWMutex
	id  int
	f   *os.File
}

func createDataFile(basePath string) (*dataFile, error) {
	id := genFileID()
	path := filepath.Join(basePath, strconv.Itoa(id)+_dataFileExt)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	return &dataFile{
		id: id,
		f:  f,
	}, nil
}

func (f *dataFile) close() error {
	return f.f.Close()
}

func genFileID() int {
	return rand.IntN(_maxFileID-_minFileID) + _minFileID
}
