package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"os"
	"sync"
)

type FID = int64

type Journal struct {
	Mu    sync.RWMutex
	Files map[FID]*File
}

type File struct {
	Mu sync.RWMutex
	ID FID
	F  *os.File
}

type Block struct {
	Signature uint64

	Timestamp int64
	Expiry    int64

	Key   []byte
	Value []byte
}

func NewBlock() *Block {
	return &Block{}
}

func (b *Block) Serialize() []byte {
	data := make([]byte, 0)

	data = binary.LittleEndian.AppendUint64(data, b.Signature)

	data = binary.LittleEndian.AppendUint64(data, uint64(b.Timestamp))
	data = binary.LittleEndian.AppendUint64(data, uint64(b.Expiry))

	data = binary.LittleEndian.AppendUint32(data, uint32(len(b.Key)))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(b.Value)))

	data = append(data, b.Key...)
	data = append(data, b.Value...)

	return data
}

func (b *Block) UnsafeSerialize() []byte {
	data := make([]byte, 0)

	data = binary.LittleEndian.AppendUint64(data, uint64(b.Timestamp))
	data = binary.LittleEndian.AppendUint64(data, uint64(b.Expiry))

	data = binary.LittleEndian.AppendUint32(data, uint32(len(b.Key)))
	data = binary.LittleEndian.AppendUint32(data, uint32(len(b.Value)))

	data = append(data, b.Key...)
	data = append(data, b.Value...)

	return data
}

func (b *Block) Deserialize(data []byte) error {
	if len(data) < b.MinSize() {
		return errors.New("invalid block size")
	}

	b.Signature = binary.LittleEndian.Uint64(data)

	b.Timestamp = int64(binary.LittleEndian.Uint64(data[8:]))
	b.Expiry = int64(binary.LittleEndian.Uint64(data[16:]))

	keySize := int(binary.LittleEndian.Uint32(data[24:]))
	valueSize := int(binary.LittleEndian.Uint32(data[28:]))

	b.Key = data[32 : 32+keySize]
	b.Value = data[32+keySize : 32+keySize+valueSize]

	return nil
}

func (*Block) MinSize() int {
	var emptyB Block
	return len(emptyB.Serialize())
}

func (*Block) GenSign(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
}

func (b *Block) SetSign() uint64 {
	data := b.UnsafeSerialize()
	b.Signature = b.GenSign(data)
	return b.Signature
}

func (b *Block) Verify() bool {
	return b.Signature == b.GenSign(b.UnsafeSerialize())
}

type Slice struct {
	Position int64
	Bytes    int
}
