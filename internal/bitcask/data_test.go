package bitcask_test

import (
	"testing"
	"time"

	"github.com/protomem/bitlog/internal/bitcask"
)

func TestBlock_Serialization(t *testing.T) {
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
}

func TestBlock_Signature(t *testing.T) {
	block := bitcask.Block{
		Timestamp: time.Now().Unix(),
		Expiry:    0,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	block.SetSign()

	if !block.Verify() {
		t.Errorf("Block is not valid")
	}
}
