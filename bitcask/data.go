package bitcask

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const (
	_activeFile int64 = 0

	_dirPerm = 0o777
)

type FileRegistry struct {
	basePath string

	mux   sync.RWMutex
	table map[int64]*DataFile
}

func NewFileRegistry(path string) (*FileRegistry, error) {
	werr := werrors.Wrap("fileReg/new")
	path = filepath.Clean(path)

	if err := os.MkdirAll(path, _dirPerm); err != nil {
		return nil, werr(err, "create base folder")
	}

	activeFile, err := CreateDataFile(path)
	if err != nil {
		return nil, werr(err)
	}

	registry := &FileRegistry{basePath: path, table: make(map[int64]*DataFile, 1)}
	registry.SetActive(activeFile)

	return registry, nil
}

func (reg *FileRegistry) GetActive() *DataFile {
	return reg.Get(_activeFile)
}

func (reg *FileRegistry) SetActive(file *DataFile) {
	if file == nil {
		return
	}

	reg.mux.Lock()
	defer reg.mux.Unlock()

	reg.table[_activeFile] = file
	reg.table[file.ID()] = file
}

func (reg *FileRegistry) Get(id int64) *DataFile {
	reg.mux.RLock()
	defer reg.mux.RUnlock()

	file, ok := reg.table[id]
	if !ok {
		return nil
	}

	return file
}

func (reg *FileRegistry) Set(file *DataFile) {
	if file == nil {
		return
	}

	reg.mux.Lock()
	defer reg.mux.Unlock()

	reg.table[file.ID()] = file
}

func (reg *FileRegistry) Remove(id int64) {
	reg.mux.Lock()
	defer reg.mux.Unlock()

	delete(reg.table, id)
}

func (reg *FileRegistry) LoadAllFiles() error {
	reg.mux.Lock()
	defer reg.mux.Unlock()

	werr := werrors.Wrap("fileReg/loadAllFiles")

	files, err := reg.openAllFiles()
	if err != nil {
		return werr(err)
	}

	for _, file := range files {
		if _, ok := reg.table[file.ID()]; ok {
			return werr(fmt.Errorf("conflict files(%d)", file.ID()), "load old files")
		}

		reg.table[file.ID()] = file
	}

	return nil
}

func (reg *FileRegistry) Range(fn func(*DataFile)) {
	reg.mux.RLock()
	defer reg.mux.RUnlock()

	files := make([]*DataFile, 0, len(reg.table))
	for _, file := range reg.table {
		files = append(files, file)
	}

	slices.SortFunc(files, func(a *DataFile, b *DataFile) int {
		return cmp.Compare(a.ID(), b.ID())
	})

	for _, file := range files {
		fn(file)
	}
}

func (reg *FileRegistry) Close() error {
	reg.mux.Lock()
	defer reg.mux.Unlock()

	files := reg.takeAllFiles()

	var errs error
	for _, file := range files {
		errs = errors.Join(errs, file.Close())
	}

	return werrors.Error(errs, "fileReg/close")
}

func (reg *FileRegistry) takeAllFiles() []*DataFile {
	files := make([]*DataFile, 0, len(reg.table))
	for id, file := range reg.table {
		if id == _activeFile {
			continue
		}

		files = append(files, file)
	}

	reg.table = make(map[int64]*DataFile)

	return files
}

func (reg *FileRegistry) openAllFiles() ([]*DataFile, error) {
	werr := werrors.Wrap("openAllFiles")

	activeFile := reg.table[_activeFile]
	fsEntries, err := os.ReadDir(reg.basePath)
	if err != nil {
		return nil, werr(err)
	}

	var (
		errs  error
		files = make([]*DataFile, 0, len(fsEntries))
	)

	for _, fsEntry := range fsEntries {
		fsEntryName := filepath.Join(reg.basePath, fsEntry.Name())

		if fsEntry.IsDir() || fsEntryName == activeFile.Name() {
			continue
		}

		if file, err := OpenDataFile(fsEntryName); err != nil {
			errs = errors.Join(errs, err)
		} else {
			files = append(files, file)
		}
	}

	return files, werr(errs)
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
	id := unixTimestamp()

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

func OpenDataFile(path string) (*DataFile, error) {
	werr := werrors.Wrap("dataFile/open")
	path = filepath.Clean(path)

	_, name := filepath.Split(path)
	id, err := parseDataFileName(name)
	if err != nil {
		return nil, werr(err)
	}

	writer := NewNopFileWriter()

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

func parseDataFileName(name string) (id int64, err error) {
	if !strings.HasSuffix(name, _dataFileExt) {
		return 0, errors.New("wrong file extension")
	}

	idStr := strings.TrimSuffix(name, _dataFileExt)
	id, err = strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, errors.New("wrong filename")
	}

	return
}

func (file *DataFile) ID() int64 {
	return file.id
}

func (file *DataFile) Name() string {
	return file.name
}

func (file *DataFile) Read(cur Cursor) (*DataEntry, error) {
	var (
		werr = werrors.Wrap("dataFile/read")
		data = make([]byte, cur.Bytes)
	)

	if _, err := file.reader.ReadAt(data, cur.Offset); err != nil {
		return nil, werr(err)
	}

	entry := new(DataEntry)
	if err := entry.Deserialize(data); err != nil {
		return nil, werr(err)
	}

	return entry, nil
}

func (file *DataFile) Write(entry *DataEntry) (Cursor, error) {
	werr := werrors.Wrap("dataFile/write")

	if entry == nil {
		return Cursor{}, nil
	}

	data := entry.Serialize()

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

type DataFileIterator struct {
	reader FileReader

	mux sync.RWMutex

	head int64
	cur  Cursor

	value *DataEntry
	err   error
}

func NewDataFileIterator(file *DataFile) (*DataFileIterator, error) {
	reader, err := OpenOSFile(file.Name())
	if err != nil {
		return nil, werrors.Error(err, "dataFileIter/new")
	}

	return &DataFileIterator{
		reader: reader,
		head:   0,
		cur:    Cursor{},
		value:  nil,
		err:    nil,
	}, nil
}

func (iter *DataFileIterator) Next() bool {
	iter.mux.Lock()
	defer iter.mux.Unlock()

	if iter.err != nil {
		return false
	}

	entry := new(DataEntry)
	read, err := entry.DeserializeFrom(iter.reader)
	if err != nil {
		iter.value = nil
		iter.cur = Cursor{}
		iter.err = err
		return false
	}

	iter.cur = Cursor{Bytes: read, Offset: iter.head}
	iter.head += int64(read)

	iter.value = entry
	iter.err = nil

	return true
}

func (iter *DataFileIterator) Result() (*DataEntry, Cursor, error) {
	iter.mux.RLock()
	defer iter.mux.RUnlock()

	return iter.value, iter.cur, werrors.Error(iter.err, "dataFileIter")
}

func (iter *DataFileIterator) Close() error {
	iter.mux.Lock()
	defer iter.mux.Unlock()

	iter.err = os.ErrClosed

	return werrors.Error(iter.err, "dataFileIter/close")
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
	return werrors.Raise(entry.DeserializeFrom(reader))
}

func (entry *DataEntry) DeserializeFrom(r io.Reader) (int, error) {
	var (
		err  error
		werr = werrors.Wrap("dataEntry/deserialize")

		totalRead int
		read      int
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
