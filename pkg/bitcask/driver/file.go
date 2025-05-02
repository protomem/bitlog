package driver

import (
	"fmt"
	"os"
	"path/filepath"
)

var _ Driver = (*File)(nil)

type File struct {
	name string
	f    *os.File
}

func CreateFile(dir string, name string) (*File, error) {
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("file/create: %w", err)
	}

	return &File{
		name: name,
		f:    f,
	}, nil
}

func (f *File) Name() string {
	return f.name
}

func (f *File) WriteAt(b []byte, offset int64) (int, error) {
	written, err := f.f.WriteAt(b, offset)
	if err != nil {
		return written, fmt.Errorf("file/write: %w", err)
	}

	return written, nil
}

func (f *File) ReadAt(b []byte, offset int64) (int, error) {
	read, err := f.f.ReadAt(b, offset)
	if err != nil {
		return read, fmt.Errorf("file/read: %w", err)
	}

	return read, nil
}

func (f *File) Close() error {
	if err := f.f.Close(); err != nil {
		return fmt.Errorf("file/close: %w", err)
	}

	return nil
}
