package bitcask

import (
	"os"
	"testing"
)

func TestDataFile_Create(t *testing.T) {
	dir := t.TempDir()

	f, err := createDataFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.close(); err != nil {
			t.Fatal(err)
		}
	}()

	if _, err := os.Stat(f.f.Name()); err != nil {
		t.Fatal(err)
	} else {
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			t.Log(entry.Name())
		}
	}
}

func TestDataFile_AppendRead(t *testing.T) {
	dir := t.TempDir()

	f, err := createDataFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.close(); err != nil {
			t.Fatal(err)
		}
	}()

	rec := dataRecord{
		crc:    1,
		tstamp: 2,
		key:    []byte("key"),
		value:  []byte("value"),
	}

	offset, written, err := f.append(rec)
	if err != nil {
		t.Fatal(err)
	}

	rec2, err := f.read(offset, written)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("rec: %v\nrec2: %v", rec, rec2)

	if rec.crc != rec2.crc {
		t.Fatal("crc mismatch")
	}
	if rec.tstamp != rec2.tstamp {
		t.Fatal("tstamp mismatch")
	}
	if string(rec.key) != string(rec2.key) {
		t.Fatal("key mismatch")
	}
	if string(rec.value) != string(rec2.value) {
		t.Fatal("value mismatch")
	}
}

func TestDataFile_AppendReadWithEmptyValue(t *testing.T) {
	dir := t.TempDir()

	f, err := createDataFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.close(); err != nil {
			t.Fatal(err)
		}
	}()

	rec := dataRecord{
		crc:    1,
		tstamp: 2,
		key:    []byte("key"),
		value:  nil,
	}

	offset, written, err := f.append(rec)
	if err != nil {
		t.Fatal(err)
	}

	rec2, err := f.read(offset, written)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("rec: %v\nrec2: %v", rec, rec2)

	if rec.crc != rec2.crc {
		t.Fatal("crc mismatch")
	}
	if rec.tstamp != rec2.tstamp {
		t.Fatal("tstamp mismatch")
	}
	if string(rec.key) != string(rec2.key) {
		t.Fatal("key mismatch")
	}
	if rec.value != nil {
		t.Fatal("value mismatch")
	}
}

func TestDataRecord_EncodeDecode(t *testing.T) {
	rec := dataRecord{
		crc:    1,
		tstamp: 2,
		key:    []byte("key"),
		value:  []byte("value"),
	}

	data := rec.encode()

	rec2 := dataRecord{}
	if err := rec2.decode(data); err != nil {
		t.Fatal(err)
	}

	t.Logf("rec: %v\nrec2: %v", rec, rec2)

	if rec.crc != rec2.crc {
		t.Fatal("crc mismatch")
	}
	if rec.tstamp != rec2.tstamp {
		t.Fatal("tstamp mismatch")
	}
	if string(rec.key) != string(rec2.key) {
		t.Fatal("key mismatch")
	}
	if string(rec.value) != string(rec2.value) {
		t.Fatal("value mismatch")
	}
}
