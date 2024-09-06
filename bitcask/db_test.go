package bitcask_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/protomem/bitlog/bitcask"
)

func TestDB(t *testing.T) {
	dir := t.TempDir()

	db, err := bitcask.Open(dir)
	if err != nil {
		t.Fatalf("failed to open db(%s): %v", dir, err)
	}

	testCases := []struct {
		name  string
		key   []byte
		value []byte
	}{}

	for i := 1; i <= 100; i++ {
		iStr := strconv.Itoa(i)
		testCases = append(testCases, struct {
			name  string
			key   []byte
			value []byte
		}{
			name:  "Case " + iStr,
			key:   []byte("key_" + iStr),
			value: []byte("value_" + iStr),
		})
	}

	for _, tC := range testCases {
		tC := tC
		t.Run(tC.name, func(t *testing.T) {
			t.Parallel()

			if err := db.Set(tC.key, tC.value, 0); err != nil {
				t.Errorf("failed to set key(%s) with value(%s): %v", tC.key, tC.value, err)
			}

			if value, err := db.Get(tC.key); err != nil {
				t.Errorf("failed to get key(%s): %v", tC.key, err)
			} else if !bytes.Equal(value, tC.value) {
				t.Errorf("failed to get key(%s): wron value", tC.key)
			}

			if err := db.Delete(tC.key); err != nil {
				t.Errorf("failed to delete key(%s): %v", tC.key, err)
			}

			if _, err := db.Get(tC.key); err == nil {
				t.Errorf("failed to get deleted key(%s)", tC.key)
			} else if !errors.Is(err, bitcask.ErrKeyNotFound) {
				t.Errorf("fialed to get deleted key(%s): unexpected error: %v", tC.key, err)
			}
		})
	}
}
