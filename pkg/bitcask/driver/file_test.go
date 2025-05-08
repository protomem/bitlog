package driver_test

import (
	"testing"

	"github.com/protomem/bitlog/pkg/bitcask/driver"
)

func Test_File(t *testing.T) {
	const expectedFile = "test_file"

	fs := driver.NewFileSystem(t.TempDir())
	_, err := fs.Driver(expectedFile)
	if err != nil {
		t.Fatalf("open/create file: unexpected error: %v", err)
	}

	fsItems, err := fs.Entries()
	if err != nil {
		t.Fatalf("read file system entries: unexpected error: %v", err)
	}

	var exists bool
	for _, fsItem := range fsItems {
		if fsItem.Name() == expectedFile {
			exists = true
			break
		}
	}

	if !exists {
		t.Fatalf("file '%s' not found", expectedFile)
	}
}
