package bitcask

import (
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

type WAL struct {
	mux    sync.Mutex
	writer FileWriter
	head   int64
}

func NewWAL(writer FileWriter) *WAL {
	return &WAL{
		writer: writer,
		head:   0,
	}
}

func (w *WAL) Write(p []byte) (int, int64, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	written, err := w.writer.WriteAt(p, w.head)
	if err != nil {
		return 0, 0, werrors.Error(err, "wal/write")
	}

	head := w.head
	w.head += int64(written)

	return written, head, nil
}
