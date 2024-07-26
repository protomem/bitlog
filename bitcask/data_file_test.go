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
