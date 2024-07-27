package bitcask

import "sync"

type keyDir struct {
	mux     sync.RWMutex
	records map[string]keyDirRecord
}

func newKeyDir() *keyDir {
	return &keyDir{
		records: make(map[string]keyDirRecord),
	}
}

func (d *keyDir) lookup(key []byte) (keyDirRecord, bool) {
	d.mux.RLock()
	defer d.mux.RUnlock()

	rec, ok := d.records[string(key)]
	return rec, ok
}

func (d *keyDir) insert(rec keyDirRecord) {
	d.mux.Lock()
	defer d.mux.Unlock()

	d.records[string(rec.key)] = rec
}

func (d *keyDir) delete(key []byte) {
	d.mux.Lock()
	defer d.mux.Unlock()

	delete(d.records, string(key))
}

type keyDirRecord struct {
	fid    int
	key    []byte
	tstamp int64
	offset int64
	size   int
}
