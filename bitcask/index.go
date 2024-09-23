package bitcask

import (
	"hash/maphash"
	"sync"
	"time"
)

type IndexState struct {
	seed maphash.Seed

	mux   sync.RWMutex
	table map[uint64]IndexEntry
}

func NewIndexState() *IndexState {
	return &IndexState{
		seed:  maphash.MakeSeed(),
		table: make(map[uint64]IndexEntry),
	}
}

func (state *IndexState) Keys() [][]byte {
	state.mux.RLock()
	defer state.mux.RUnlock()

	if len(state.table) == 0 {
		return nil
	}

	keys := make([][]byte, 0, len(state.table))
	for _, value := range state.table {
		copyKey := append([]byte{}, value.Key...)
		keys = append(keys, copyKey)
	}

	return keys
}

func (state *IndexState) Find(key []byte) (IndexEntry, bool) {
	state.mux.RLock()
	defer state.mux.RUnlock()

	hashKey := maphash.Bytes(state.seed, key)
	idx, ok := state.table[hashKey]

	return idx.Clone(), ok
}

func (state *IndexState) Insert(idx IndexEntry) {
	state.mux.Lock()
	defer state.mux.Unlock()

	hashKey := maphash.Bytes(state.seed, idx.Key)
	state.table[hashKey] = idx.Clone()
}

func (state *IndexState) Remove(key []byte) {
	state.mux.Lock()
	defer state.mux.Unlock()

	hashKey := maphash.Bytes(state.seed, key)
	delete(state.table, hashKey)
}

func (state *IndexState) Clear() {
	state.mux.Lock()
	defer state.mux.Unlock()

	state.table = make(map[uint64]IndexEntry)
}

type IndexEntry struct {
	File    int64
	Created time.Time
	Key     []byte
	Cursor  Cursor
}

func NewIndexEntry(file int64, created time.Time, key []byte, cur Cursor) IndexEntry {
	return IndexEntry{
		File:    file,
		Created: created,
		Key:     key,
		Cursor:  cur,
	}
}

func (idx IndexEntry) Clone() IndexEntry {
	return IndexEntry{
		File:    idx.File,
		Created: idx.Created,
		Key:     append([]byte{}, idx.Key...),
		Cursor:  idx.Cursor,
	}
}
