package bitcask

import (
	"os"
	"sync"
)

type CID = int64

type Journal struct {
	Mu sync.RWMutex
}

type Cluster struct {
	Mu sync.RWMutex
	ID CID
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
