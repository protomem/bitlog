package bitcask_test

import (
	"testing"

	"github.com/protomem/bitlog/bitcask"
)

func TestDB(t *testing.T) {
	dir := t.TempDir()

	db, err := bitcask.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	testData := []struct {
		key   string
		value string
	}{
		{
			key:   "key",
			value: "value",
		},
		{
			key:   "key2",
			value: "value2",
		},
		{
			key:   "key3",
			value: "value3",
		},
	}

	for _, d := range testData {
		if err := db.Put([]byte(d.key), []byte(d.value)); err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range testData {
		value, err := db.Get([]byte(d.key))
		if err != nil {
			t.Fatal(err)
		}
		if string(value) != d.value {
			t.Fatalf("value mismatch")
		}
	}

	for _, d := range testData {
		if err := db.Delete([]byte(d.key)); err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range testData {
		_, err := db.Get([]byte(d.key))
		if err != bitcask.ErrKeyNotFound {
			if err == nil {
				t.Fatalf("key should not exist")
			} else {
				t.Fatal(err)
			}
		}
	}
}
