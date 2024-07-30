package bitcask

import (
	"bufio"
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
	mux  sync.RWMutex
	id   int
	f    *os.File
	head int64
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
		id:   id,
		f:    f,
		head: stat.Size(),
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

	return &dataFile{
		id:   id,
		f:    f,
		head: 0,
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

	var (
		offset int64
		r      = bufio.NewReader(f.f)
	)

	for offset <= f.head {
		var rec dataRecord
		read, err := rec.streamDecode(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if err := fn(rec, offset, read); err != nil {
			return err
		}

		offset += int64(read)
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
	crc := crc32.ChecksumIEEE(bytes.Join([][]byte{key, value}, nil))
	return dataRecord{
		crc:    crc,
		tstamp: time.Now().UnixMicro(),
		key:    key,
		value:  value,
	}
}

func newDataGrave(key []byte) dataRecord {
	return newDataRecord(key, nil)
}

func (r *dataRecord) verify() error {
	crc := crc32.ChecksumIEEE(bytes.Join([][]byte{r.key, r.value}, nil))
	if crc != r.crc {
		return ErrInvalidData
	}
	return nil
}

func (r *dataRecord) encode() []byte {
	data := make([]byte, 4+8+4+4+len(r.key)+len(r.value))

	binary.LittleEndian.PutUint32(data, r.crc)
	binary.LittleEndian.PutUint64(data[4:12], uint64(r.tstamp))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(r.key)))
	binary.LittleEndian.PutUint32(data[16:20], uint32(len(r.value)))
	copy(data[20:20+len(r.key)], r.key)
	copy(data[20+len(r.key):], r.value)

	return data
}

func (r *dataRecord) decode(data []byte) error {
	if len(data) < 20 {
		return ErrInvalidDataSize
	}

	r.crc = binary.LittleEndian.Uint32(data)
	r.tstamp = int64(binary.LittleEndian.Uint64(data[4:12]))
	r.key = data[20 : 20+binary.LittleEndian.Uint32(data[12:16])]
	r.value = data[20+binary.LittleEndian.Uint32(data[12:16]):]

	return nil
}

func (r *dataRecord) streamDecode(src io.Reader) (int, error) {
	header := make([]byte, 20)

	var (
		read      int
		totalRead int
		err       error
	)

	read, err = src.Read(header)
	if err != nil {
		return 0, err
	}
	totalRead += read

	r.crc = binary.LittleEndian.Uint32(header)
	r.tstamp = int64(binary.LittleEndian.Uint64(header[4:12]))

	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])

	data := make([]byte, keySize+valueSize)
	read, err = src.Read(data)
	if err != nil {
		return 0, err
	}
	totalRead += read

	r.key = data[:keySize]
	r.value = data[keySize:]

	return totalRead, nil
}

func (r *dataRecord) isGrave() bool {
	return len(r.value) == 0
}
