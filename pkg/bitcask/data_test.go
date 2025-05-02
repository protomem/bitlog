package bitcask_test

import (
	"fmt"
	"testing"

	"github.com/protomem/bitlog/pkg/bitcask"
	"github.com/protomem/bitlog/pkg/bitcask/driver"
)

func Test_ParseDriverName(t *testing.T) {
	expectedID := bitcask.GenBucketID()
	driverName := bitcask.FmtDriverName(expectedID)

	actualID, err := bitcask.ParseDriverName(driverName)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if actualID != expectedID {
		t.Fatalf("expected %d, got %d", expectedID, actualID)
	}
}

func TestBlock_Serialization(t *testing.T) {
	expectedBlock := bitcask.NewBlock([]byte("key"), []byte("value"))

	data := expectedBlock.Serialize()

	actualBlock := new(bitcask.Block)
	err := actualBlock.Deserialize(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !expectedBlock.Equals(actualBlock) {
		t.Fatalf("expected %+v, got %+v", expectedBlock, actualBlock)
	}
}

func TestBlock_CheckSignature(t *testing.T) {
	block := bitcask.NewBlock([]byte("key"), []byte("value"))

	if err := block.Sign(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := block.Verify(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBlock_CheckSignature_Fail(t *testing.T) {
	block := bitcask.NewBlock([]byte("key"), []byte("value"))

	if err := block.Sign(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	block.Key = []byte("wrong key")
	block.Value = []byte("wrong value")

	if err := block.Verify(); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestBucket_WriteRead(t *testing.T) {
	var file *driver.File
	{
		var err error

		id := bitcask.GenBucketID()
		name := bitcask.FmtDriverName(id)

		file, err = driver.CreateFile(t.TempDir(), name)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	defer func() {
		_ = file.Close()
	}()

	bucket, err := bitcask.NewBucket(file)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	testData := []struct {
		block *bitcask.Block
		ref   bitcask.Reference
	}{}

	// Generate test data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)

		testData = append(testData, struct {
			block *bitcask.Block
			ref   bitcask.Reference
		}{
			block: bitcask.NewBlock([]byte(key), []byte(value)),
			ref:   bitcask.Reference{},
		})
	}

	// Write test data
	for i, data := range testData {
		ref, err := bucket.Write(data.block)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		testData[i].ref = ref
	}

	// Reverse test data
	for i, j := 0, len(testData)-1; i < j; i, j = i+1, j-1 {
		testData[i], testData[j] = testData[j], testData[i]
	}

	// Read test data
	for _, data := range testData {
		block, err := bucket.Read(data.ref)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !block.Equals(data.block) {
			t.Fatalf("expected %+v, got %+v", data.block, block)
		}
	}
}

func TestCluster_ActiveBucket(t *testing.T) {
	driverf := driver.NewFileFactory(t.TempDir())
	cluster := bitcask.NewCluster(driverf)

	numBucket := 10

	for i := 0; i < numBucket; i++ {
		if err := cluster.CreateActiveBucket(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := cluster.GetActiveBucket(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	entries, err := driverf.Entries()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, entry := range entries {
		d, err := driver.OpenFile(driverf.Root(), entry.Name())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := bitcask.NewBucket(d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(entries) != numBucket {
		t.Fatalf("expected %d, got %d", numBucket, len(entries))
	}
}
