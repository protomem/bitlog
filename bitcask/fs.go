package bitcask

import (
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/protomem/bitlog/pkg/werrors"
)

const (
	_filePerm    = 0o644
	_dataFileExt = ".data"
)

type FileReader interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type FileWriter interface {
	io.Writer
	io.WriterAt
	io.Closer
}

var _ FileWriter = NopFileWriter{}

type NopFileWriter struct{}

func NewNopFileWriter() NopFileWriter {
	return NopFileWriter{}
}

// Write implements FileWriter.
func (NopFileWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

// WriteAt implements FileWriter.
func (NopFileWriter) WriteAt(p []byte, _ int64) (int, error) {
	return len(p), nil
}

// Close implements FileWriter.
func (NopFileWriter) Close() error {
	return nil
}

var (
	_ FileReader = (*OSFile)(nil)
	_ FileWriter = (*OSFile)(nil)
)

type OSFile struct {
	f *os.File
}

func CreateOSFile(name string) (*OSFile, error) {
	return NewOpenOSFile(name, os.O_CREATE|os.O_RDWR, _filePerm)
}

func OpenOSFile(name string) (*OSFile, error) {
	return NewOpenOSFile(name, os.O_RDONLY, _filePerm)
}

func NewOpenOSFile(name string, flags int, perm fs.FileMode) (*OSFile, error) {
	f, err := os.OpenFile(name, flags, perm)
	if err != nil {
		return nil, fmt.Errorf("osfile/new(%s): %w", name, err)
	}

	return &OSFile{f: f}, nil
}

// Read implements FileReader.
func (f *OSFile) Read(p []byte) (int, error) {
	read, err := f.f.Read(p)
	return read, werrors.Error(err, "osfile/read")
}

// ReadAt implements FileReader.
func (f *OSFile) ReadAt(p []byte, off int64) (int, error) {
	read, err := f.f.ReadAt(p, off)
	return read, werrors.Error(err, "osfile/readAt")
}

// Write implements FileWriter.
func (f *OSFile) Write(p []byte) (int, error) {
	written, err := f.f.Write(p)
	return written, werrors.Error(err, "osfile/write")
}

// WriteAt implements FileWriter.
func (f *OSFile) WriteAt(p []byte, off int64) (int, error) {
	written, err := f.f.WriteAt(p, off)
	return written, werrors.Error(err, "osfile/writeAt")
}

// Close implements FileReader and FileWriter.
func (f *OSFile) Close() error {
	err := f.f.Close()
	return werrors.Error(err, "osfile/close")
}
