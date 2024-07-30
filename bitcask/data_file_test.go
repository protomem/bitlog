package bitcask

import (
	"bytes"
	"testing"
)

func TestDataFile_Foreach(t *testing.T) {
	var err error

	dir := t.TempDir()
	file, err := createDataFile(dir)
	if err != nil {
		t.Fatal(err)
	}

	testData := []dataRecord{
		newDataRecord([]byte("key1"), []byte("value1_1")),
		newDataRecord([]byte("key2"), []byte("value2_1")),
		newDataGrave([]byte("key1")),
		newDataRecord([]byte("key1"), []byte("value1_2")),
	}

	for _, rec := range testData {
		_, _, err := file.append(rec)
		if err != nil {
			t.Fatal(err)
		}
	}

	recs := make([]dataRecord, 0)
	err = file.foreach(func(rec dataRecord, offset int64, size int) error {
		recs = append(recs, rec)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(recs) != len(testData) {
		t.Fatal("length actual data and test data does not match")
	}
	for i := 0; i < len(testData); i++ {
		t.Logf("expected: %v, actual: %v", testData[i], recs[i])
		if (!bytes.Equal(testData[i].key, recs[i].key) || !bytes.Equal(testData[i].value, recs[i].value)) ||
			(testData[i].isGrave() != recs[i].isGrave()) {
			t.Logf("expected: %v, actual: %v", testData[i], recs[i])
		}
	}
}

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
