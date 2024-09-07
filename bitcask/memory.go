package bitcask

import (
	"hash/maphash"
	"sync"
	"time"
)

type MemTable struct {
	seed maphash.Seed

	mux   sync.RWMutex
	table map[uint64]Index
}

func NewMemTable() *MemTable {
	return &MemTable{
		seed:  maphash.MakeSeed(),
		table: make(map[uint64]Index),
	}
}

func (t *MemTable) Keys() [][]byte {
	t.mux.RLock()
	defer t.mux.RUnlock()

	if len(t.table) == 0 {
		return nil
	}

	keys := make([][]byte, 0, len(t.table))
	for _, value := range t.table {
		copyKey := append([]byte{}, value.Key...)
		keys = append(keys, copyKey)
	}

	return keys
}

func (t *MemTable) Find(key []byte) (Index, bool) {
	t.mux.RLock()
	defer t.mux.RUnlock()

	hashKey := maphash.Bytes(t.seed, key)
	idx, ok := t.table[hashKey]

	return idx.Clone(), ok
}

func (t *MemTable) Insert(idx Index) {
	t.mux.Lock()
	defer t.mux.Unlock()

	hashKey := maphash.Bytes(t.seed, idx.Key)
	t.table[hashKey] = idx.Clone()
}

func (t *MemTable) Remove(key []byte) {
	t.mux.Lock()
	defer t.mux.Unlock()

	hashKey := maphash.Bytes(t.seed, key)
	delete(t.table, hashKey)
}

func (t *MemTable) Clear() {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.table = make(map[uint64]Index)
}

type Index struct {
	File    int64
	Created time.Time
	Key     []byte
	Cursor  Cursor
}

func NewIndex(file int64, created time.Time, key []byte, cur Cursor) Index {
	return Index{
		File:    file,
		Created: created,
		Key:     key,
		Cursor:  cur,
	}
}

func (idx Index) Clone() Index {
	return Index{
		File:    idx.File,
		Created: idx.Created,
		Key:     append([]byte{}, idx.Key...),
		Cursor:  idx.Cursor,
	}
}
