package bitcask

import "sync"

type DB struct {
	opts options

	idx *Index
	jrn *Journal
}

func New(opts ...Option) (*DB, error) {
	appliedOpts, err := applyOptions(opts...)
	if err != nil {
		return nil, err
	}

	db := &DB{
		opts: appliedOpts,
		idx:  NewIndex(),
		jrn:  NewJournal(),
	}

	return db, nil
}

type Entry struct {
	Timestamp int64
	Key       []byte
	Value     []byte
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

type Journal struct {
	mu      sync.RWMutex
	entries []Entry
}

func NewJournal() *Journal {
	return &Journal{}
}
