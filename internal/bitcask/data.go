package bitcask

import (
	"encoding/binary"
	"hash/crc64"
	"io"
	"os"
	"sync"
)

const (
	_blockUnsafeHeaderSize = 24
	_blockHeaderSize       = 32
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

func (b *Block) UnsafeSerializeHeaderTo(dest []byte) error {
	if len(dest) < _blockUnsafeHeaderSize {
		return io.ErrShortBuffer
	}

	binary.LittleEndian.PutUint64(dest[0:8], uint64(b.Timestamp))
	binary.LittleEndian.PutUint64(dest[8:16], uint64(b.Expiry))

	binary.LittleEndian.PutUint32(dest[16:20], uint32(len(b.Key)))
	binary.LittleEndian.PutUint32(dest[20:24], uint32(len(b.Value)))

	return nil
}

func (b *Block) SerializeHeaderTo(dest []byte) error {
	if len(dest) < _blockHeaderSize {
		return io.ErrShortBuffer
	}

	binary.LittleEndian.PutUint64(dest, b.Signature)

	return b.UnsafeSerializeHeaderTo(dest[8:])
}

func (b *Block) SerializeBodyTo(dest []byte) error {
	if len(dest) < len(b.Key)+len(b.Value) {
		return io.ErrShortBuffer
	}

	copy(dest, b.Key)
	copy(dest[len(b.Key):], b.Value)

	return nil
}

func (b *Block) UnsafeSerializeTo(dest []byte) error {
	if err := b.UnsafeSerializeHeaderTo(dest); err != nil {
		return err
	}

	if len(dest) < _blockUnsafeHeaderSize+len(b.Key)+len(b.Value) {
		return io.ErrShortBuffer
	}

	if err := b.SerializeBodyTo(dest[_blockUnsafeHeaderSize:]); err != nil {
		return err
	}

	return nil
}

func (b *Block) SerializeTo(dest []byte) error {
	if err := b.SerializeHeaderTo(dest); err != nil {
		return err
	}

	if len(dest) < _blockHeaderSize+len(b.Key)+len(b.Value) {
		return io.ErrShortBuffer
	}

	if err := b.SerializeBodyTo(dest[_blockHeaderSize:]); err != nil {
		return err
	}

	return nil
}

func (b *Block) Serialize() []byte {
	data := make([]byte, _blockHeaderSize+len(b.Key)+len(b.Value))
	_ = b.SerializeTo(data)
	return data
}

func (b *Block) UnsafeSerialize() []byte {
	data := make([]byte, _blockUnsafeHeaderSize+len(b.Key)+len(b.Value))
	_ = b.UnsafeSerializeTo(data)
	return data
}

func (b *Block) UnsafeDeserialize(data []byte) error {
	if len(data) < _blockUnsafeHeaderSize {
		return io.ErrShortBuffer
	}

	b.Timestamp = int64(binary.LittleEndian.Uint64(data[0:8]))
	b.Expiry = int64(binary.LittleEndian.Uint64(data[8:16]))

	keySize := int(binary.LittleEndian.Uint32(data[16:20]))
	valueSize := int(binary.LittleEndian.Uint32(data[20:24]))

	if len(data) < _blockUnsafeHeaderSize+keySize+valueSize {
		return io.ErrShortBuffer
	}

	b.Key = append([]byte{}, data[24:24+keySize]...)
	b.Value = append([]byte{}, data[24+keySize:24+keySize+valueSize]...)

	return nil
}

func (b *Block) Deserialize(data []byte) error {
	if len(data) < _blockHeaderSize {
		return io.ErrShortBuffer
	}

	b.Signature = binary.LittleEndian.Uint64(data[0:8])

	if err := b.UnsafeDeserialize(data[8:]); err != nil {
		return err
	}

	return nil
}

func (*Block) GenSign(data []byte) uint64 {
	return crc64.Checksum(data, crc64.MakeTable(crc64.ISO))
}

func (b *Block) SetSign() uint64 {
	data := b.UnsafeSerialize()
	b.Signature = b.GenSign(data)
	return b.Signature
}

func (b *Block) CheckSign() bool {
	return b.Signature == b.GenSign(b.UnsafeSerialize())
}

type Slice struct {
	Position int64
	Bytes    int
}
