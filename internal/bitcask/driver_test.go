package bitcask_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/protomem/bitlog/internal/bitcask"
)

func TestFile(t *testing.T) {
	testDir := t.TempDir()

	var driver *bitcask.File
	{
		var err error

		fileName := filepath.Join(testDir, "test.db")
		driver, err = bitcask.OpenFile(fileName)
		if err != nil {
			t.Fatalf("Failed open file driver: err=%s", err)
		}
	}
	defer func() {
		if err := driver.Close(); err != nil {
			t.Errorf("Failed to close driver: err=%s", err)
		}
	}()

	for i := 0; i < 9; i++ {
		data := []byte(fmt.Sprintf("data%d", i))

		if _, err := driver.WriteAt(data, int64(len(data)*i)); err != nil {
			t.Errorf("Failed to write data: data=%s err=%s", data, err)
		}
	}

	for i := 0; i < 9; i++ {
		data := make([]byte, 5)
		if _, err := driver.ReadAt(data, int64(len(data)*i)); err != nil {
			t.Errorf("Failed to read data: data=%s err=%s", data, err)
		}

		if string(data) != fmt.Sprintf("data%d", i) {
			t.Errorf("Failed to read data: data=%s", data)
		}
	}
}

func TestBuffer(t *testing.T) {
	driver := bitcask.NewBuffer()

	for i := 0; i < 9; i++ {
		data := []byte(fmt.Sprintf("data%d", i))

		if _, err := driver.WriteAt(data, int64(len(data)*i)); err != nil {
			t.Errorf("Failed to write data: data=%s err=%s", data, err)
		}
	}

	for i := 0; i < 9; i++ {
		data := make([]byte, 5)
		if _, err := driver.ReadAt(data, int64(len(data)*i)); err != nil {
			t.Errorf("Failed to read data: data=%s err=%s", data, err)
		}

		if string(data) != fmt.Sprintf("data%d", i) {
			t.Errorf("Failed to read data: data=%s", data)
		}
	}
}
