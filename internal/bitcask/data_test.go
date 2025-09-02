package bitcask_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/protomem/bitlog/internal/bitcask"
	"github.com/protomem/bitlog/pkg/crand"
)

func TestFileName(t *testing.T) {
	t.Run("Base case", func(t *testing.T) {
		fid := bitcask.FID(crand.Range(1000, 9999))

		originFileName := bitcask.FormatFileName(bitcask.FID(fid))

		parsedFID, err := bitcask.ParseFileName(originFileName)
		if err != nil {
			t.Fatalf("Failed to parse file name: %v", err)
		}
		if parsedFID != fid {
			t.Errorf("Expected FID %d, got %d", fid, parsedFID)
		}
	})

	t.Run("Long path", func(t *testing.T) {
		preffix := "/some/long/path"
		fid := bitcask.FID(crand.Range(1000, 9999))

		originFileName := bitcask.FormatFileName(bitcask.FID(fid))
		longpath := filepath.Join(preffix, originFileName)

		parsedFID, err := bitcask.ParseFileName(longpath)
		if err != nil {
			t.Fatalf("Failed to parse file name: %v", err)
		}
		if parsedFID != fid {
			t.Errorf("Expected FID %d, got %d", fid, parsedFID)
		}
	})
}

func TestBlock_Serialization(t *testing.T) {
	t.Run("Base case", func(t *testing.T) {
		block := bitcask.Block{
			Timestamp: time.Now().Unix(),
			Expiry:    0,
			Key:       []byte("key"),
			Value:     []byte("value"),
		}

		rawBlock := block.Serialize()

		var newBlock bitcask.Block
		if err := newBlock.Deserialize(rawBlock); err != nil {
			t.Fatalf("Failed to deserialize block: %v", err)
		}

		if newBlock.Timestamp != block.Timestamp {
			t.Errorf("Expected timestamp %d, got %d", block.Timestamp, newBlock.Timestamp)
		}
		if newBlock.Expiry != block.Expiry {
			t.Errorf("Expected expiry %d, got %d", block.Expiry, newBlock.Expiry)
		}
		if string(newBlock.Key) != string(block.Key) {
			t.Errorf("Expected key %s, got %s", string(block.Key), string(newBlock.Key))
		}
		if string(newBlock.Value) != string(block.Value) {
			t.Errorf("Expected value %s, got %s", string(block.Value), string(newBlock.Value))
		}
	})
}

func TestBlock_Signature(t *testing.T) {
	t.Run("Base case", func(t *testing.T) {
		block := bitcask.Block{
			Timestamp: time.Now().Unix(),
			Expiry:    0,
			Key:       []byte("key"),
			Value:     []byte("value"),
		}

		block.SetSign()

		if !block.CheckSign() {
			t.Errorf("Block is not valid")
		}
	})
}
