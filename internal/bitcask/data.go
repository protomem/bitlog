package bitcask

import (
	"os"
	"sync"
)

type FID = int64

type Journal struct {
	Mu    sync.RWMutex
	Files map[FID]*File
}

type File struct {
	Mu sync.RWMutex
	ID FID
	F  *os.File
}

type Block struct {
	Signature int64

	Timestamp int64
	Expiry    int64

	Key   []byte
	Value []byte
}

type Slice struct {
	Position int64
	Bytes    int
}
