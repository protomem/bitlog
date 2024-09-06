package bitcask_test

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/protomem/bitlog/bitcask"
)

func TestBlob_SignAndVerify(t *testing.T) {
	blob := bitcask.NewBlob(time.Now(), time.Now().Add(5*time.Hour), []byte("some_key"), []byte("some_value"))

	blob.CRC = blob.Sign()
	if !blob.Verify() {
		t.Fatalf("failed to verify blob(%+v)", blob)
	}
}

func TestBlob_Serialization(t *testing.T) {
	blob := bitcask.NewBlob(time.Now(), time.Now().Add(5*time.Hour), []byte("some_key"), []byte("some_value"))
	data := blob.Serialize()

	decodedBlob := new(bitcask.Blob)
	if err := decodedBlob.Deserialize(data); err != nil {
		t.Fatalf("failed to deserialize blob: %v", err)
	}
}

func TestBlobFile_CreateAndOpen(t *testing.T) {
	path := t.TempDir()

	var (
		err  error
		file *bitcask.BlobFile
	)

	file, err = bitcask.CreateBlobFile(path)
	if err != nil {
		t.Fatalf("failed to create blob file in %s: %v", path, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close blob file(%s) after create: %v", file.Name(), err)
	}

	path = file.Name()

	file, err = bitcask.OpenBlobFile(path)
	if err != nil {
		t.Fatalf("failed to open blob file(%s): %v", path, err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("failed to close blob file(%s) after open: %v", file.Name(), err)
	}
}

func TestBlobFile_WriteAndRead(t *testing.T) {
	path := t.TempDir()

	file, err := bitcask.CreateBlobFile(path)
	if err != nil {
		t.Fatalf("failed to create blob file in %s: %v", path, err)
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

			writeBlob := bitcask.NewBlob(time.Time{}, time.Time{}, tC.key, tC.value)

			cursor, err := file.Write(writeBlob)
			if err != nil {
				t.Fatalf("failed to write blob(%+v): %v", writeBlob, err)
			}

			readBlob, err := file.Read(cursor)
			if err != nil {
				t.Fatalf("failed to read blob by cursor(%+v): %v", cursor, err)
			}

			if !readBlob.Verify() {
				t.Errorf("failed to verify blob(%+v)", readBlob)
			}

			if !bytes.Equal(readBlob.Key, writeBlob.Key) {
				t.Errorf("failed to compare keys: %s and %s", readBlob.Key, writeBlob.Key)
			}
			if !bytes.Equal(readBlob.Value, writeBlob.Value) {
				t.Errorf("failed to compare values: %s and %s", readBlob.Value, writeBlob.Value)
			}
		})
	}
}
