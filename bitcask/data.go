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
)

const (
	_activeFile  int64 = 0
	_rangeFileID int64 = 1_000_000_000

	_dirPerm = 0o777

	_filePerm    = 0o644
	_fileBlobExt = ".data"
)

var (
	ErrWrongBytes = errors.New("wrong bytes")
	ErrWrongFile  = errors.New("wrong file")
)

type SSTable struct {
	mux   sync.RWMutex
	table map[int64]*BlobFile
}

func NewSSTable(path string) (*SSTable, error) {
	if err := os.MkdirAll(path, _dirPerm); err != nil {
		return nil, err
	}

	activeFile, err := CreateBlobFile(path)
	if err != nil {
		return nil, err
	}

	table := &SSTable{table: make(map[int64]*BlobFile, 1)}
	table.SetActive(activeFile)

	return table, nil
}

func (t *SSTable) GetActive() *BlobFile {
	return t.Get(_activeFile)
}

func (t *SSTable) SetActive(file *BlobFile) {
	if file == nil {
		return
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.table[_activeFile] = file
	t.table[file.ID()] = file
}

func (t *SSTable) Get(id int64) *BlobFile {
	t.mux.RLock()
	defer t.mux.RUnlock()

	file, ok := t.table[id]
	if !ok {
		return nil
	}

	return file
}

func (t *SSTable) Set(file *BlobFile) {
	if file == nil {
		return
	}

	t.mux.Lock()
	defer t.mux.Unlock()

	t.table[file.ID()] = file
}

func (t *SSTable) Remove(id int64) {
	t.mux.Lock()
	defer t.mux.Unlock()

	delete(t.table, id)
}

func (t *SSTable) LoadAllFiles() error {
	// TODO: Implement
	return nil
}

type Cursor struct {
	Bytes  int
	Offset int64
}

type BlobFile struct {
	id   int64
	name string

	reader *fileReader
	writer *fileWriter
}

func CreateBlobFile(path string) (*BlobFile, error) {
	id := genFileID()

	name := strconv.FormatInt(id, 10) + _fileBlobExt
	path = filepath.Join(path, name)

	writer, err := newFileWriter(path)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(path)
	if err != nil {
		return nil, err
	}

	return &BlobFile{
		id:     id,
		name:   path,
		reader: reader,
		writer: writer,
	}, nil
}

func genFileID() int64 {
	min := _rangeFileID
	max := (_rangeFileID * 10) - 1
	return rand.Int64N(max-min) + min
}

func OpenBlobFile(path string) (*BlobFile, error) {
	_, name := filepath.Split(path)
	if !strings.HasSuffix(name, _fileBlobExt) {
		return nil, ErrWrongFile
	}

	idStr := strings.TrimSuffix(name, _fileBlobExt)
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, ErrWrongFile
	}

	writer, err := newFileWriter(path)
	if err != nil {
		return nil, err
	}

	reader, err := newFileReader(path)
	if err != nil {
		return nil, err
	}

	return &BlobFile{
		id:     id,
		name:   path,
		reader: reader,
		writer: writer,
	}, nil
}

func (file *BlobFile) ID() int64 {
	return file.id
}

func (file *BlobFile) Name() string {
	return file.name
}

func (file *BlobFile) Read(cursor Cursor) (*Blob, error) {
	data, err := file.reader.read(cursor.Bytes, cursor.Offset)
	if err != nil {
		return nil, err
	}

	blob := new(Blob)
	if err := blob.Deserialize(data); err != nil {
		return nil, err
	}

	return blob, nil
}

func (file *BlobFile) Write(blob *Blob) (Cursor, error) {
	if blob == nil {
		return Cursor{}, nil
	}

	data := blob.Serialize()

	var (
		cursor Cursor
		err    error
	)

	cursor.Bytes, cursor.Offset, err = file.writer.write(data)
	if err != nil {
		return Cursor{}, err
	}

	return cursor, nil
}

func (file *BlobFile) Close() error {
	var errs error
	errs = errors.Join(errs, file.writer.close())
	errs = errors.Join(errs, file.reader.close())
	return errs
}

type Blob struct {
	CRC     uint64
	Created time.Time
	Expired time.Time
	Key     []byte
	Value   []byte
}

func NewBlob(created, expired time.Time, key, value []byte) *Blob {
	blob := &Blob{
		CRC:     0,
		Created: created,
		Expired: expired,
		Key:     key,
		Value:   value,
	}
	blob.CRC = blob.Sign()
	return blob
}

func (b *Blob) Sign() uint64 {
	data := b.Serialize()[8:] // truncate CRC bytes
	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}

func (b *Blob) Verify() bool {
	check := b.Sign()
	return check == b.CRC
}

func (b *Blob) Serialize() []byte {
	data := make([]byte, 32+len(b.Key)+len(b.Value))

	binary.LittleEndian.PutUint64(data[:8], b.CRC)

	binary.LittleEndian.PutUint64(data[8:16], uint64(b.Created.Unix()))
	binary.LittleEndian.PutUint64(data[16:24], uint64(b.Expired.Unix()))

	binary.LittleEndian.PutUint32(data[24:28], uint32(len(b.Key)))
	binary.LittleEndian.PutUint32(data[28:32], uint32(len(b.Value)))

	copy(data[32:32+len(b.Key)], b.Key)
	copy(data[32+len(b.Key):], b.Value)

	return data
}

func (b *Blob) Deserialize(data []byte) error {
	if len(data) < 32 {
		return ErrWrongBytes
	}

	b.CRC = binary.LittleEndian.Uint64(data[:8])

	created := int64(binary.LittleEndian.Uint64(data[8:16]))
	expired := int64(binary.LittleEndian.Uint64(data[16:24]))

	b.Created = time.Unix(created, 0)
	b.Expired = time.Unix(expired, 0)

	key := int(binary.LittleEndian.Uint32(data[24:28]))
	value := int(binary.LittleEndian.Uint32(data[28:32]))

	if len(data) != 32+key+value {
		return ErrWrongBytes
	}

	b.Key = make([]byte, key)
	b.Value = make([]byte, value)

	copy(b.Key, data[32:32+key])
	copy(b.Value, data[32+key:])

	return nil
}

func (b *Blob) IsGrave() bool {
	return len(b.Value) == 0
}

func (b *Blob) IsExpired() bool {
	return b.Expired != time.Time{} && b.Expired.After(b.Created)
}

type fileReader struct {
	f *os.File
}

func newFileReader(path string) (*fileReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, _filePerm)
	if err != nil {
		return nil, err
	}

	return &fileReader{f: f}, nil
}

func (file *fileReader) modTime() (time.Time, error) {
	info, err := file.f.Stat()
	if err != nil {
		return time.Time{}, err
	}

	return info.ModTime(), nil
}

func (file *fileReader) read(bytes int, offset int64) ([]byte, error) {
	b := make([]byte, bytes)

	read, err := file.f.ReadAt(b, offset)
	if err != nil {
		return nil, err
	}
	if bytes != read {
		return nil, ErrWrongBytes
	}

	return b, nil
}

func (file *fileReader) close() error {
	return file.f.Close()
}

type fileWriter struct {
	mux  sync.RWMutex
	f    *os.File
	head int64
}

func newFileWriter(path string) (*fileWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, _filePerm)
	if err != nil {
		return nil, err
	}

	return &fileWriter{
		f:    f,
		head: 0,
	}, nil
}

func (file *fileWriter) modTime() (time.Time, error) {
	file.mux.RLock()
	defer file.mux.RUnlock()

	info, err := file.f.Stat()
	if err != nil {
		return time.Time{}, err
	}

	return info.ModTime(), nil
}

func (file *fileWriter) write(b []byte) (written int, offset int64, err error) {
	file.mux.Lock()
	defer file.mux.Unlock()

	offset = file.head

	written, err = file.f.WriteAt(b, offset)
	if err != nil {
		return
	}
	if len(b) != written {
		err = ErrWrongBytes
		return
	}

	file.head += int64(written)

	return
}

func (file *fileWriter) close() error {
	return file.f.Close()
}
