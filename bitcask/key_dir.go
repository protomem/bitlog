package bitcask

import "sync"

type keyDir struct {
	mux     sync.RWMutex
	records map[string]idxRecord
}

func newKeyDir() *keyDir {
	return &keyDir{
		records: make(map[string]idxRecord),
	}
}

func (d *keyDir) allKeys() [][]byte {
	d.mux.RLock()
	defer d.mux.RUnlock()

	keys := make([][]byte, 0, len(d.records))

	for key := range d.records {
		keys = append(keys, []byte(key))
	}

	return keys
}

func (d *keyDir) lookup(key []byte) (idxRecord, bool) {
	d.mux.RLock()
	defer d.mux.RUnlock()

	rec, ok := d.records[string(key)]
	return rec, ok
}

func (d *keyDir) insert(rec idxRecord) {
	d.mux.Lock()
	defer d.mux.Unlock()

	d.records[string(rec.key)] = rec
}

func (d *keyDir) delete(key []byte) {
	d.mux.Lock()
	defer d.mux.Unlock()

	delete(d.records, string(key))
}

type idxRecord struct {
	fid    int
	key    []byte
	tstamp int64
	offset int64
	size   int
}
