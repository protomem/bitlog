package binlog

import "sync"

type Index struct {
	lock sync.RWMutex

	Key   []byte
	LogID LogID
}

func NewIndex(key []byte, logID LogID) *Index {
	return &Index{
		Key:   key,
		LogID: logID,
	}
}

type KeyDir struct {
	mu   sync.RWMutex
	keys map[string]*Index
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		keys: make(map[string]*Index),
	}
}

func (k *KeyDir) Lookup(key []byte) (*Index, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	index, ok := k.lookup(key)

	return index, ok
}

func (k *KeyDir) Insert(index *Index) (exists bool) {
	if index == nil {
		return
	}

	k.mu.Lock()
	defer k.mu.Unlock()

	key := string(index.Key)
	if _, ok := k.keys[key]; ok {
		exists = true
	}

	k.keys[key] = index

	return
}

func (k *KeyDir) Remove(key []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()

	index, ok := k.lookup(key)
	if !ok || index == nil {
		return
	}

	delete(k.keys, string(key))
}

func (k *KeyDir) lookup(key []byte) (*Index, bool) {
	index, ok := k.keys[string(key)]
	if !ok || index == nil {
		return nil, false
	}

	return index, true
}
