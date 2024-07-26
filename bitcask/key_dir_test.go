package bitcask

import "testing"

func TestKeyDir(t *testing.T) {
	index := newKeyDir()

	index.insert(keyDirRecord{
		fid:    1,
		key:    []byte("key"),
		tstamp: 2,
		offset: 3,
		size:   4,
	})

	rec, ok := index.lookup([]byte("key"))
	if !ok {
		t.Fatal("key not found")
	}
	if rec.fid != 1 {
		t.Fatal("fid mismatch")
	}
	if string(rec.key) != "key" {
		t.Fatal("key mismatch")
	}
	if rec.tstamp != 2 {
		t.Fatal("tstamp mismatch")
	}
	if rec.offset != 3 {
		t.Fatal("offset mismatch")
	}
	if rec.size != 4 {
		t.Fatal("size mismatch")
	}
}
