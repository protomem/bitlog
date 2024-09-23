package bitcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const (
	_activeFile  int64 = 0
	_rangeFileID int64 = 1_000_000_000

	_dirPerm = 0o777
)

var ErrFileNotFound = errors.New("file not found")

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

type Cursor struct {
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
		return nil, werr(ErrFileNotFound, "wrong file extension")
	}

	idStr := strings.TrimSuffix(name, _dataFileExt)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, werr(ErrFileNotFound, "wrong filename")
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

func (file *DataFile) Read(cur Cursor) (*DataEntry, error) {
	werr := werrors.Wrap("dataFile/read")
	data := make([]byte, cur.Bytes)

	_, err := file.reader.ReadAt(data, cur.Offset)
	if err != nil {
		return nil, werr(err)
	}

	dentry := new(DataEntry)
	if err := dentry.Deserialize(data); err != nil {
		return nil, werr(err)
	}

	return dentry, nil
}

func (file *DataFile) Write(dentry *DataEntry) (Cursor, error) {
	werr := werrors.Wrap("dataFile/write")

	if dentry == nil {
		return Cursor{}, nil
	}

	data := dentry.Serialize()

	var (
		err error
		cur Cursor
	)

	cur.Bytes, cur.Offset, err = file.wal.Write(data)
	if err != nil {
		return Cursor{}, werr(err)
	}

	return cur, nil
}

func (file *DataFile) Close() error {
	var errs error
	errs = errors.Join(errs, file.writer.Close())
	errs = errors.Join(errs, file.reader.Close())
	return errs
}

type DataEntry struct {
	Checksum uint64
	Created  int64
	Expired  int64
	Key      []byte
	Value    []byte
}

func NewDataEntry(created, expired int64, key, value []byte) *DataEntry {
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

func NewTombstone(created int64, key []byte) *DataEntry {
	return NewDataEntry(created, 0, key, []byte{})
}

func (entry *DataEntry) Sign() uint64 {
	data := entry.Serialize()[8:] // truncate CRC bytes
	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}

func (entry *DataEntry) IsVerify() bool {
	checksum := entry.Sign()
	return checksum == entry.Checksum
}

func (entry *DataEntry) Equal(otherEntry *DataEntry) bool {
	if otherEntry == nil {
		return false
	}

	if entry.Created != otherEntry.Created || entry.Expired != otherEntry.Expired ||
		!bytes.Equal(entry.Key, otherEntry.Key) || !bytes.Equal(entry.Value, otherEntry.Value) {
		return false
	}

	return true
}

func (entry *DataEntry) Serialize() []byte {
	data := make([]byte, 32+len(entry.Key)+len(entry.Value))

	binary.LittleEndian.PutUint64(data[:8], entry.Checksum)

	binary.LittleEndian.PutUint64(data[8:16], uint64(entry.Created))
	binary.LittleEndian.PutUint64(data[16:24], uint64(entry.Expired))

	binary.LittleEndian.PutUint32(data[24:28], uint32(len(entry.Key)))
	binary.LittleEndian.PutUint32(data[28:32], uint32(len(entry.Value)))

	copy(data[32:32+len(entry.Key)], entry.Key)
	copy(data[32+len(entry.Key):], entry.Value)

	return data
}

func (entry *DataEntry) SerializeTo(w io.Writer) (int, error) {
	data := entry.Serialize()
	written, err := w.Write(data)
	return written, werrors.Error(err, "dataEntry/serialize")
}

func (entry *DataEntry) Deserialize(data []byte) error {
	reader := bytes.NewReader(data)
	_, err := entry.DeserializeFrom(reader)
	return err
}

func (entry *DataEntry) DeserializeFrom(r io.Reader) (int, error) {
	var (
		werr      = werrors.Wrap("dataEntry/deserialize")
		totalRead = 0

		err  error
		read int
	)

	head := make([]byte, 32)
	read, err = r.Read(head)
	totalRead += read

	if err != nil {
		return totalRead, werr(err)
	}
	if read != 32 {
		return totalRead, io.ErrUnexpectedEOF
	}

	entry.Checksum = binary.LittleEndian.Uint64(head[:8])

	entry.Created = int64(binary.LittleEndian.Uint64(head[8:16]))
	entry.Expired = int64(binary.LittleEndian.Uint64(head[16:24]))

	keyLen := int(binary.LittleEndian.Uint32(head[24:28]))
	valueLen := int(binary.LittleEndian.Uint32(head[28:32]))

	body := make([]byte, keyLen+valueLen)
	read, err = r.Read(body)
	totalRead += read

	if err != nil {
		return totalRead, werr(err)
	}
	if read != keyLen+valueLen {
		return totalRead, io.ErrUnexpectedEOF
	}

	entry.Key = body[:keyLen]
	entry.Value = body[keyLen:]

	return totalRead, nil
}

func (entry *DataEntry) IsTombstone() bool {
	return len(entry.Value) == 0
}

func (entry *DataEntry) IsExpired() bool {
	return entry.Expired != 0 && entry.Expired > entry.Created
}
