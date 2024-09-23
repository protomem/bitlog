package bitcask_test

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/protomem/bitlog/bitcask"
)

func TestDataEntry_SignAndVerify(t *testing.T) {
	dentry := bitcask.NewDataEntry(time.Now().UnixMilli(), time.Now().Add(5*time.Hour).UnixMilli(), []byte("some_key"), []byte("some_value"))

	dentry.Checksum = dentry.Sign()
	if !dentry.Verify() {
		t.Fatalf("failed to verify data entry(%+v)", dentry)
	}
}

func TestDataEntry_Serialization(t *testing.T) {
	dentry := bitcask.NewDataEntry(time.Now().UnixMilli(), time.Now().Add(5*time.Hour).UnixMilli(), []byte("some_key"), []byte("some_value"))
	data := dentry.Serialize()

	decodedEntry := new(bitcask.DataEntry)
	if err := decodedEntry.Deserialize(data); err != nil {
		t.Fatalf("failed to deserialize data entry: %v", err)
	}
}

func TestDataFile_CreateAndOpen(t *testing.T) {
	path := t.TempDir()

	var (
		err  error
		file *bitcask.DataFile
	)

	file, err = bitcask.CreateDataFile(path)
	if err != nil {
		t.Fatalf("failed to create data file in %s: %v", path, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close data file(%s) after create: %v", file.Name(), err)
	}

	path = file.Name()

	file, err = bitcask.OpenDataFile(path)
	if err != nil {
		t.Fatalf("failed to open data file(%s): %v", path, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close data file(%s) after open: %v", file.Name(), err)
	}
}

func TestDataFile_WriteAndRead(t *testing.T) {
	path := t.TempDir()

	file, err := bitcask.CreateDataFile(path)
	if err != nil {
		t.Fatalf("failed to create data file in %s: %v", path, err)
	}

	testCases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "Without value",
			key:   []byte("key"),
			value: []byte{},
		},
		{
			name:  "Without key",
			key:   []byte{},
			value: []byte("value"),
		},
	}

	for i := 1; i <= 10; i++ {
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

			writeDentry := bitcask.NewDataEntry(0, 0, tC.key, tC.value)

			cursor, err := file.Write(writeDentry)
			if err != nil {
				t.Fatalf("failed to write data entry(%+v): %v", writeDentry, err)
			}

			readDentry, err := file.Read(cursor)
			if err != nil {
				t.Fatalf("failed to read data entry by cursor(%+v): %v", cursor, err)
			}

			if !readDentry.Verify() {
				t.Errorf("failed to verify data entry(%+v)", readDentry)
			}

			if !bytes.Equal(readDentry.Key, writeDentry.Key) {
				t.Errorf("failed to compare keys: %s and %s", readDentry.Key, writeDentry.Key)
			}
			if !bytes.Equal(readDentry.Value, writeDentry.Value) {
				t.Errorf("failed to compare values: %s and %s", readDentry.Value, writeDentry.Value)
			}
		})
	}
}
