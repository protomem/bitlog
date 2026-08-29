package buffer

import (
	"errors"
	"io"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const _dynBufErrorMsg = "buffer.Dynamic"

var ErrNegativeOffset = errors.New("negative offset")

type Dynamic struct {
	mu  sync.RWMutex
	buf []byte
}

func NewDynamic(buf []byte) *Dynamic {
	return &Dynamic{buf: buf}
}

func (d *Dynamic) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, werrors.Error(ErrNegativeOffset, _dynBufErrorMsg, "writerAt")
	}

	end := int(off) + len(p)

	d.mu.Lock()
	defer d.mu.Unlock()

	if end > len(d.buf) {
		if end <= cap(d.buf) {
			// Grow slice within existing capacity.
			d.buf = d.buf[:end]
		} else {
			// Allocate new buffer with required size.
			newBuf := make([]byte, end)
			copy(newBuf, d.buf)
			d.buf = newBuf
		}
	}

	copy(d.buf[int(off):end], p)
	return len(p), nil
}

func (d *Dynamic) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, werrors.Error(ErrNegativeOffset, _dynBufErrorMsg, "readAt")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	if off >= int64(len(d.buf)) {
		return 0, io.EOF
	}

	available := len(d.buf) - int(off)
	if len(p) > available {
		n = available
	} else {
		n = len(p)
	}

	copy(p, d.buf[int(off):int(off)+n])

	if n < len(p) {
		return n, io.EOF
	}

	return n, nil
}
