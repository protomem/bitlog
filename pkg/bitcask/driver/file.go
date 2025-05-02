package driver

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

var (
	_ DriverFactory = (*FileFactory)(nil)
	_ Driver        = (*File)(nil)
)

type FileFactory struct {
	root string
}

func NewFileFactory(root string) *FileFactory {
	root = filepath.Clean(root)

	return &FileFactory{
		root: root,
	}
}

func (ff *FileFactory) Driver(name string) (Driver, error) {
	return CreateFile(ff.root, name)
}

func (ff *FileFactory) Root() string {
	return ff.root
}

func (ff *FileFactory) Entries() ([]fs.DirEntry, error) {
	entries, err := os.ReadDir(ff.root)
	if err != nil {
		return nil, fmt.Errorf("file/entries: %w", err)
	}

	return entries, nil
}

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

func OpenFile(dir string, name string) (*File, error) {
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		return nil, fmt.Errorf("file/open: %w", err)
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
