package bitcask_test

import (
	"iter"
	"testing"
	"time"

	"github.com/protomem/bitlog/bitcask"
)

func TestDataEntry_SignAndVerify(t *testing.T) {
	entry := bitcask.NewDataEntry(time.Now().UnixMilli(), time.Now().Add(5*time.Hour).UnixMilli(), []byte("some_key"), []byte("some_value"))

	entry.Checksum = entry.Sign()
	if !entry.IsVerify() {
		t.Fatalf("failed to verify data entry(%+v)", entry)
	}
}

func TestDataEntry_Serialization(t *testing.T) {
	entry := bitcask.NewDataEntry(time.Now().UnixMilli(), time.Now().Add(5*time.Hour).UnixMilli(), []byte("some_key"), []byte("some_value"))
	data := entry.Serialize()

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

func FuzzDataFile_WriteAndRead(f *testing.F) {
	path := f.TempDir()

	file, err := bitcask.CreateDataFile(path)
	if err != nil {
		f.Fatalf("failed to create data file in %s: %v", path, err)
	}

	f.Add([]byte("key"), []byte("value"))
	f.Add([]byte("key"), []byte{})
	f.Add([]byte{}, []byte("value"))

	f.Fuzz(func(t *testing.T, key []byte, value []byte) {
		t.Parallel()

		writeEntry := bitcask.NewDataEntry(time.Now().UnixMilli(), 0, key, value)

		cur, err := file.Write(writeEntry)
		if err != nil {
			t.Fatalf("failed write data entry(%+v): %v", writeEntry, err)
		}

		readEntry, err := file.Read(cur)
		if err != nil {
			t.Fatalf("failed read data entry by cursor(%+v): %v", cur, readEntry)
		}

		if !readEntry.IsVerify() {
			t.Errorf("failed verify data entry(%+v)", readEntry)
		}

		if !writeEntry.Equal(readEntry) {
			t.Errorf("failed compare data entry for write(%+v) and read(%+v)", writeEntry, readEntry)
		}
	})
}
