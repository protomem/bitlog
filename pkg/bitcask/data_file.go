package bitcask

import (
	"os"
	"sync"
	"time"
)

type DataFile struct {
	mux  sync.Mutex
	file *os.File
}

func OpenDataFile(path string) (*DataFile, error) {
	return &DataFile{}, nil
}

func CreateDataFile(path string) (*DataFile, error) {
	return &DataFile{}, nil
}

func (f *DataFile) Read(offset int64, size int) (DataRecord, error) {
	return DataRecord{}, nil
}

func (f *DataFile) Write(record DataRecord) error {
	return nil
}

func (f *DataFile) Close() error {
	return nil
}

type DataRecord struct {
	CRC       uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

func NewDataRecord(key []byte, value []byte) DataRecord {
	now := time.Now().Unix()
	return DataRecord{
		CRC:       0,
		Timestamp: now,
		Key:       key,
		Value:     value,
	}
}
