package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protomem/bitlog/pkg/werrors"
)

const (
	_activeFile  int64 = 0
	_rangeFileID int64 = 1_000_000_000

	_dirPerm = 0o777
)

var (
	ErrWrongBytes   = errors.New("wrong bytes")
	ErrWrongFile    = errors.New("wrong file")
	ErrFileNotFound = errors.New("file not found")
	ErrInvalidValue = errors.New("invalid value")
)

type FileRegistry struct {
	mux   sync.RWMutex
	table map[int64]*DataFile
}

func NewFileRegistry(path string) (*FileRegistry, error) {
	if err := os.MkdirAll(path, _dirPerm); err != nil {
		return nil, err
	}

	activeFile, err := CreateDataFile(path)
	if err != nil {
		return nil, err
	}

	registry := &FileRegistry{table: make(map[int64]*DataFile, 1)}
	registry.SetActive(activeFile)

	return registry, nil
}

func (r *FileRegistry) GetActive() *DataFile {
	return r.Get(_activeFile)
}

func (r *FileRegistry) SetActive(file *DataFile) {
	if file == nil {
		return
	}

	r.mux.Lock()
	defer r.mux.Unlock()

	r.table[_activeFile] = file
	r.table[file.ID()] = file
}

func (r *FileRegistry) Get(id int64) *DataFile {
	r.mux.RLock()
	defer r.mux.RUnlock()

	file, ok := r.table[id]
	if !ok {
		return nil
	}

	return file
}

func (r *FileRegistry) Set(file *DataFile) {
	if file == nil {
		return
	}

	r.mux.Lock()
	defer r.mux.Unlock()

	r.table[file.ID()] = file
}

func (r *FileRegistry) Remove(id int64) {
	r.mux.Lock()
	defer r.mux.Unlock()

	delete(r.table, id)
}

func (*FileRegistry) LoadAllFiles() error {
	// TODO: Implement
	return nil
}

func (r *FileRegistry) Close() error {
	r.mux.Lock()
	defer r.mux.Unlock()

	files := make([]*DataFile, 0, len(r.table))
	for id, file := range r.table {
		if id == _activeFile {
			continue
		}

		files = append(files, file)
	}

	r.table = make(map[int64]*DataFile)

	var errs error
	for _, file := range files {
		errs = errors.Join(errs, file.Close())
	}

	return errs
}

type FileReference struct {
	Bytes  int
	Offset int64
}

type DataFile struct {
	id   int64
	name string

	reader FileReader
	writer FileWriter

	wal *WAL
}

func CreateDataFile(path string) (*DataFile, error) {
	werr := werrors.Wrap("dataFile/create")
	id := genFileID()

	name := strconv.FormatInt(id, 10) + _dataFileExt
	path = filepath.Join(path, name)

	writer, err := CreateOSFile(path)
	if err != nil {
		return nil, werr(err)
	}

	reader, err := OpenOSFile(path)
	if err != nil {
		return nil, werr(err)
	}

	return &DataFile{
		id:     id,
		name:   path,
		reader: reader,
		writer: writer,
		wal:    NewWAL(writer),
	}, nil
}

func genFileID() int64 {
	min := _rangeFileID
	max := (_rangeFileID * 10) - 1
	return rand.Int64N(max-min) + min
}

func OpenDataFile(path string) (*DataFile, error) {
	werr := werrors.Wrap("dataFile/open")

	_, name := filepath.Split(path)
	if !strings.HasSuffix(name, _dataFileExt) {
		return nil, werr(ErrWrongFile)
	}

	idStr := strings.TrimSuffix(name, _dataFileExt)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, werr(ErrWrongFile)
	}

	writer := NewNopFileWriter()

	reader, err := OpenOSFile(path)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		id:     id,
		name:   path,
		reader: reader,
		writer: writer,
		wal:    NewWAL(writer),
	}, nil
}

func (file *DataFile) ID() int64 {
	return file.id
}

func (file *DataFile) Name() string {
	return file.name
}

func (file *DataFile) Read(ref FileReference) (*DataEntry, error) {
	werr := werrors.Wrap("dataFile/read")
	data := make([]byte, ref.Bytes)

	_, err := file.reader.ReadAt(data, ref.Offset)
	if err != nil {
		return nil, werr(err)
	}

	dentry := new(DataEntry)
	if err := dentry.Deserialize(data); err != nil {
		return nil, werr(err)
	}

	return dentry, nil
}

func (file *DataFile) Write(dentry *DataEntry) (FileReference, error) {
	werr := werrors.Wrap("dataFile/write")

	if dentry == nil {
		return FileReference{}, nil
	}

	data := dentry.Serialize()

	var (
		ref FileReference
		err error
	)

	ref.Bytes, ref.Offset, err = file.wal.Write(data)
	if err != nil {
		return FileReference{}, werr(err)
	}

	return ref, nil
}

func (file *DataFile) Close() error {
	var errs error
	errs = errors.Join(errs, file.writer.Close())
	errs = errors.Join(errs, file.reader.Close())
	return errs
}

type DataEntry struct {
	Checksum uint64
	Created  time.Time
	Expired  time.Time
	Key      []byte
	Value    []byte
}

func NewDataEntry(created, expired time.Time, key, value []byte) *DataEntry {
	dentry := &DataEntry{
		Checksum: 0,
		Created:  created,
		Expired:  expired,
		Key:      key,
		Value:    value,
	}
	dentry.Checksum = dentry.Sign()
	return dentry
}

func NewTombstone(created time.Time, key []byte) *DataEntry {
	return NewDataEntry(created, time.Time{}, key, []byte{})
}

func (entry *DataEntry) Sign() uint64 {
	data := entry.Serialize()[8:] // truncate CRC bytes
	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}

func (entry *DataEntry) Verify() bool {
	checksum := entry.Sign()
	return checksum == entry.Checksum
}

func (entry *DataEntry) Serialize() []byte {
	data := make([]byte, 32+len(entry.Key)+len(entry.Value))

	binary.LittleEndian.PutUint64(data[:8], entry.Checksum)

	binary.LittleEndian.PutUint64(data[8:16], uint64(entry.Created.Unix()))
	binary.LittleEndian.PutUint64(data[16:24], uint64(entry.Expired.Unix()))

	binary.LittleEndian.PutUint32(data[24:28], uint32(len(entry.Key)))
	binary.LittleEndian.PutUint32(data[28:32], uint32(len(entry.Value)))

	copy(data[32:32+len(entry.Key)], entry.Key)
	copy(data[32+len(entry.Key):], entry.Value)

	return data
}

func (entry *DataEntry) Deserialize(data []byte) error {
	if len(data) < 32 {
		return ErrWrongBytes
	}

	entry.Checksum = binary.LittleEndian.Uint64(data[:8])

	created := int64(binary.LittleEndian.Uint64(data[8:16]))
	expired := int64(binary.LittleEndian.Uint64(data[16:24]))

	entry.Created = time.Unix(created, 0)
	entry.Expired = time.Unix(expired, 0)

	key := int(binary.LittleEndian.Uint32(data[24:28]))
	value := int(binary.LittleEndian.Uint32(data[28:32]))

	if len(data) != 32+key+value {
		return ErrWrongBytes
	}

	entry.Key = make([]byte, key)
	entry.Value = make([]byte, value)

	copy(entry.Key, data[32:32+key])
	copy(entry.Value, data[32+key:])

	return nil
}

func (entry *DataEntry) IsTombstone() bool {
	return len(entry.Value) == 0
}

func (entry *DataEntry) IsExpired() bool {
	return entry.Expired != time.Time{} && entry.Expired.After(entry.Created)
}
