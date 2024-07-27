package bitcask_test

import (
	"testing"

	"github.com/protomem/bitlog/bitcask"
)

func FuzzDB(f *testing.F) {
	dir := f.TempDir()

	db, err := bitcask.Open(dir)
	if err != nil {
		f.Fatal(err)
	}

	testCases := []struct {
		Key   []byte
		Value []byte
	}{
		{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
		{
			Key:   nil,
			Value: nil,
		},
		{
			Key:   []byte("key"),
			Value: nil,
		},
		{
			Key:   nil,
			Value: []byte("value"),
		},
	}
	for _, tC := range testCases {
		f.Add(tC.Key, tC.Value)
	}

	f.Fuzz(func(t *testing.T, testKey []byte, testValue []byte) {
		if err := db.Put(testKey, testValue); err != nil {
			if err == bitcask.ErrInvalidKeyOrValueSize &&
				(len(testKey) == 0 || len(testValue) == 0) {
				return
			}

			t.Fatal(err)
		}

		if value, err := db.Get(testKey); err != nil {
			t.Fatal(err)
		} else if string(value) != string(testValue) {
			t.Fatal("value mismatch")
		}

		if err := db.Delete(testKey); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Get(testKey); err != bitcask.ErrKeyNotFound {
			t.Fatal("key should not exist")
		}
	})
}
