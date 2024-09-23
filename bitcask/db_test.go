package bitcask_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/protomem/bitlog/bitcask"
)

func FuzzDB(f *testing.F) {
	dir := f.TempDir()

	db, err := bitcask.Open(dir)
	if err != nil {
		f.Fatalf("failed to open db(%s): %v", dir, err)
	}

	f.Fuzz(func(t *testing.T, key []byte, value []byte) {
		t.Parallel()

		if len(key) == 0 || len(value) == 0 {
			t.Skip()
		}

		if err := db.Set(key, value, 0); err != nil {
			t.Fatalf("failed to set key(%s) with value(%s): %v", key, value, err)
		}

		if readValue, err := db.Get(key); err != nil {
			t.Fatalf("failed to get key(%s): %v", key, err)
		} else if !bytes.Equal(readValue, value) {
			t.Fatalf("failed to get key(%s): expected value='%s', actual value='%s'", key, value, readValue)
		}

		if err := db.Delete(key); err != nil {
			t.Fatalf("failed to delete key(%s): %v", key, err)
		}

		if _, err := db.Get(key); err == nil {
			t.Fatalf("failed to get deleted key(%s)", key)
		} else if !errors.Is(err, bitcask.ErrKeyNotFound) {
			t.Fatalf("fialed to get deleted key(%s): unexpected error: %v", key, err)
		}
	})
}
