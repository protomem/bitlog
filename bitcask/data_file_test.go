package bitcask

import (
	"bytes"
	"testing"
)

func TestDataRecord(t *testing.T) {
	testCases := []struct {
		Name  string
		Key   []byte
		Value []byte
		Grave bool
	}{
		{
			Name:  "Success",
			Key:   []byte("key"),
			Value: []byte("value"),
			Grave: false,
		},
		{
			Name:  "Seccess: Grave",
			Key:   []byte("key"),
			Value: []byte{},
			Grave: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.Name, func(t *testing.T) {
			rec := newDataRecord(tC.Key, tC.Value)
			data := rec.encode()

			rec2 := dataRecord{}
			if err := rec2.decode(data); err != nil {
				t.Fatal(err)
			}

			if err := rec2.verify(); err != nil {
				t.Fatal(err)
			}

			if rec2.isGrave() != tC.Grave {
				t.Fatal("should not be grave")
			}

			if !bytes.Equal(rec.key, rec2.key) {
				t.Fatal("key not equal")
			}

			if !bytes.Equal(rec.value, rec2.value) {
				t.Fatal("value not equal")
			}
		})
	}
}
