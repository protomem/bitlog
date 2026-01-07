package database

import "sync"

type Entry struct {
	Key []byte
	Ref Reference
}

type Index struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewIndex() *Index {
	return &Index{
		entries: make(map[string]Entry),
	}
}

func (idx *Index) Lookup(key []byte) (Entry, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entry, ok := idx.entries[string(key)]
	return entry, ok
}

func (idx *Index) Insert(entry Entry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entries[string(entry.Key)] = entry
}

func (idx *Index) Remove(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.entries, key)
}
