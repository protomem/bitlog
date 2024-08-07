package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	_dataFileExt = ".data"

	_minFileID = 100000
	_maxFileID = 999999
)

var (
	ErrInvalidDataSize = errors.New("invalid data size")
	ErrInvalidData     = errors.New("invalid data")
	ErrInvalidFile     = errors.New("invalid file")
)

type dataFile struct {
	mux    sync.RWMutex
	id     int
	f      *os.File
	head   int64
	tstamp int64
}

func createDataFile(basePath string) (*dataFile, error) {
	id := genFileID()
	path := filepath.Join(basePath, strconv.Itoa(id)+_dataFileExt)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &dataFile{
		id:     id,
		f:      f,
		head:   0,
		tstamp: stat.ModTime().Unix(),
	}, nil
}

func openDataFile(filename string) (*dataFile, error) {
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	id, err := parseFilename(filename)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &dataFile{
		id:     id,
		f:      f,
		head:   stat.Size(),
		tstamp: stat.ModTime().Unix(),
	}, nil
}

func (f *dataFile) close() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	return f.f.Close()
}

func (f *dataFile) append(rec dataRecord) (int64, int, error) {
	f.mux.Lock()
	defer f.mux.Unlock()

	data := rec.encode()
	offset := f.head

	written, err := f.f.WriteAt(data, offset)
	f.head += int64(written)

	return offset, written, err
}

func (f *dataFile) read(offset int64, size int) (dataRecord, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	data := make([]byte, size)
	if _, err := f.f.ReadAt(data, offset); err != nil {
		return dataRecord{}, err
	}

	rec := dataRecord{}
	if err := rec.decode(data); err != nil {
		return dataRecord{}, err
	}

	return rec, nil
}

func (f *dataFile) foreach(fn func(data dataRecord, offset int64, size int) error) error {
	f.mux.RLock()
	defer f.mux.RUnlock()

	var offset int64

	for offset < f.head {
		var rec dataRecord
		if err := rec.streamDecode(f.f); err != nil {
			return err
		}

		if err := fn(rec, offset, rec.size()); err != nil {
			return err
		}

		offset += int64(rec.size())
	}

	return nil
}

func genFileID() int {
	return rand.IntN(_maxFileID-_minFileID) + _minFileID
}

func parseFilename(filename string) (fid int, err error) {
	base := filepath.Base(filename)
	if ext := filepath.Ext(base); ext != _dataFileExt {
		return 0, ErrInvalidFile
	}

	base = base[:len(base)-len(_dataFileExt)]

	fid, err = strconv.Atoi(base)
	if err != nil {
		return 0, ErrInvalidFile
	}

	return fid, nil
}

type dataRecord struct {
	crc    uint32 // 4 bytes
	tstamp int64  // 8 bytes
	key    []byte
	value  []byte
}

func newDataRecord(key, value []byte) dataRecord {
	now := time.Now().UnixMilli()
	rec := dataRecord{
		tstamp: now,
		key:    key,
		value:  value,
	}
	rec.crc = rec.sign()
	return rec
}

func newDataGrave(key []byte) dataRecord {
	return newDataRecord(key, nil)
}

func (r *dataRecord) sizeHeader() int {
	return 20
}

func (r *dataRecord) sizeBody() int {
	return len(r.key) + len(r.value)
}

func (r *dataRecord) size() int {
	return r.sizeHeader() + r.sizeBody()
}

func (r *dataRecord) sign() uint32 {
	return crc32.ChecksumIEEE(bytes.Join([][]byte{int64ToBytes(r.tstamp), r.key, r.value}, nil))
}

func (r *dataRecord) verify() error {
	crc := r.sign()
	if crc != r.crc {
		return ErrInvalidData
	}
	return nil
}

func (r *dataRecord) encode() []byte {
	data := make([]byte, r.size())

	binary.LittleEndian.PutUint32(data, r.crc)
	binary.LittleEndian.PutUint64(data[4:12], uint64(r.tstamp))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(data[16:20], uint32(len(r.value)))
	copy(data[20:20+len(r.key)], r.key)
	copy(data[20+len(r.key):], r.value)

	return data
}

func (r *dataRecord) decode(data []byte) error {
	if err := r.decodeHeader(data[:r.sizeHeader()]); err != nil {
		return err
	}

	if err := r.decodeBody(data[r.sizeHeader():]); err != nil {
		return err
	}

	return nil
}

func (r *dataRecord) decodeHeader(data []byte) error {
	if len(data) != r.sizeHeader() {
		return ErrInvalidDataSize
	}

	r.crc = binary.LittleEndian.Uint32(data)
	r.tstamp = int64(binary.LittleEndian.Uint64(data[4:12]))
	r.key = make([]byte, binary.LittleEndian.Uint32(data[12:16]))
	r.value = make([]byte, binary.LittleEndian.Uint32(data[16:20]))

	return nil
}

func (r *dataRecord) decodeBody(data []byte) error {
	if len(data) != r.sizeBody() {
		return ErrInvalidDataSize
	}

	copy(r.key, data)
	if len(r.value) > 0 {
		copy(r.value, data[len(r.key):])
	}

	return nil
}

func (r *dataRecord) streamDecode(src io.Reader) error {
	header := make([]byte, r.sizeHeader())
	if _, err := src.Read(header); err != nil {
		return nil
	}

	if err := r.decodeHeader(header); err != nil {
		return err
	}

	body := make([]byte, r.sizeBody())
	if _, err := src.Read(body); err != nil {
		return err
	}

	if err := r.decodeBody(body); err != nil {
		return err
	}

	return nil
}

func (r *dataRecord) isGrave() bool {
	return len(r.value) == 0
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}
