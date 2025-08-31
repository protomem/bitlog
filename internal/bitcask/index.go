package bitcask

import "sync"

type Index struct {
	Mu      sync.RWMutex
	Records map[string]*Record
}

type Record struct {
	Key []byte

	File FID
	Ref  Slice
}
