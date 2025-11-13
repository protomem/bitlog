package bitcask

import (
	"io"
	"os"
)

var (
	_ ReadDriver  = (*File)(nil)
	_ WriteDriver = (*File)(nil)

	_ ReadDriver  = (*Buffer)(nil)
	_ WriteDriver = (*Buffer)(nil)
)

type ReadDriver interface {
	io.ReaderAt
	Size() int64
}

type WriteDriver interface {
	io.WriterAt
}

type File struct {
	f *os.File
}

func OpenFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	return &File{f: f}, nil
}

// ReadAt implements Driver.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	return f.f.ReadAt(p, off)
}

// WriteAt implements Driver.
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	return f.f.WriteAt(p, off)
}

// Size implements Driver.
func (f *File) Size() int64 {
	info, err := f.f.Stat()
	if err != nil {
		return -1
	}
	return info.Size()
}

func (f *File) Close() error {
	return f.f.Close()
}

type Buffer struct {
	b []byte
}

func NewBuffer() *Buffer {
	return &Buffer{b: make([]byte, 0)}
}

func (b *Buffer) Append(p []byte) {
	b.b = append(b.b, p...)
}

func (b *Buffer) Bytes() []byte {
	return append([]byte{}, b.b...)
}

// ReadAt implements Driver.
func (b *Buffer) ReadAt(p []byte, off int64) (n int, err error) {
	if int(off) > len(b.b) {
		return 0, io.EOF
	}
	return copy(p, b.b[off:]), nil
}

// WriteAt implements Driver.
func (b *Buffer) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > len(b.b) {
		b.tryGrow(int(off) + len(p))
	}
	return copy(b.b[off:], p), nil
}

// Size implements Driver.
func (b *Buffer) Size() int64 {
	return int64(len(b.b))
}

func (b *Buffer) tryGrow(n int) {
	if len(b.b) < n {
		b.b = append(b.b, make([]byte, n)...)
	}
}
