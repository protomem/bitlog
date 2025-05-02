package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protomem/bitlog/pkg/bitcask/driver"
	"github.com/protomem/bitlog/pkg/crand"
)

var (
	ErrInvalidSignature = fmt.Errorf("invalid signature")
	ErrWrongBytes       = fmt.Errorf("wrong bytes")
)

type Block struct {
	Signature uint64 // CRC64, 8 bytes
	Timestamp int64  // Unix timestamp, 8 bytes
	Key       []byte
	Value     []byte
}

func NewBlock(key, value []byte) *Block {
	if len(key) == 0 {
		panic("block: key is empty")
	}

	return &Block{
		Signature: 0,
		Timestamp: NowTimestamp(),
		Key:       key,
		Value:     value,
	}
}

func NowTimestamp() int64 {
	return time.Now().UnixMilli()
}

func (b *Block) Serialize() []byte {
	data := make([]byte, 8+8+4+4+len(b.Key)+len(b.Value))

	binary.LittleEndian.PutUint64(data, b.Signature)
	binary.LittleEndian.PutUint64(data[8:], uint64(b.Timestamp))

	binary.LittleEndian.PutUint32(data[16:], uint32(len(b.Key)))
	binary.LittleEndian.PutUint32(data[20:], uint32(len(b.Value)))

	copy(data[24:], b.Key)
	copy(data[24+len(b.Key):], b.Value)

	return data
}

func (b *Block) Deserialize(data []byte) error {
	const op = "block/deserialize"

	if len(data) < 24 {
		return fmt.Errorf("%s: %w", op, ErrWrongBytes)
	}

	b.Signature = binary.LittleEndian.Uint64(data)
	b.Timestamp = int64(binary.LittleEndian.Uint64(data[8:]))

	keyLen := int(binary.LittleEndian.Uint32(data[16:]))
	valueLen := int(binary.LittleEndian.Uint32(data[20:]))

	if len(data) != 24+keyLen+valueLen {
		return fmt.Errorf("%s: %w", op, ErrWrongBytes)
	}

	b.Key = append(b.Key, data[24:24+keyLen]...)
	b.Value = append(b.Value, data[24+keyLen:]...)

	return nil
}

func (b *Block) Sign() error {
	b.Signature = b.genSignature()
	return nil
}

func (b *Block) Verify() error {
	if b.genSignature() != b.Signature {
		return fmt.Errorf("block/verify: %w", ErrInvalidSignature)
	}

	return nil
}

// genSignature generates block signature, without Signature field
func (b *Block) genSignature() uint64 {
	data := b.Serialize()
	sign := crc64.Checksum(data[8:], crc64.MakeTable(crc64.ECMA))
	return sign
}

func (b *Block) Equals(other *Block) bool {
	if other == nil {
		return false
	}

	return b.Timestamp == other.Timestamp &&
		string(b.Key) == string(other.Key) &&
		string(b.Value) == string(other.Value)
}

type Reference struct {
	Offset int64
	Size   int
}

type Bucket struct {
	ID int64

	wal *WriteAheadLog
}

func NewBucket(driver driver.Driver) (*Bucket, error) {
	const op = "bucket/new"

	if driver == nil {
		panic(fmt.Sprintf("%s: driver is nil", op))
	}

	id, err := ParseDriverName(driver.Name())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &Bucket{
		ID:  id,
		wal: NewWriteAheadLog(driver),
	}, nil
}

func GenBucketID() int64 {
	return crand.GenInt64(12)
}

func FmtDriverName(id int64) string {
	return fmt.Sprintf("%s.blob", strconv.FormatInt(id, 10))
}

func ParseDriverName(name string) (int64, error) {
	_, filename := filepath.Split(name)
	filename = strings.TrimSuffix(filename, filepath.Ext(filename))

	id, err := strconv.ParseInt(filename, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse driver name: %w", err)
	}

	return id, nil
}

func (b *Bucket) Write(block *Block) (Reference, error) {
	const op = "bucket/write"

	if block == nil {
		panic(fmt.Sprintf("%s: block is nil", op))
	}

	if err := block.Sign(); err != nil {
		return Reference{}, fmt.Errorf("%s: sign block: %w", op, err)
	}

	rawBlock := block.Serialize()

	ref, err := b.wal.Write(rawBlock)
	if err != nil {
		return Reference{}, fmt.Errorf("%s: %w", op, err)
	}

	return ref, nil
}

func (b *Bucket) Read(ref Reference) (*Block, error) {
	const op = "bucket/read"

	rawBlock, err := b.wal.Read(ref)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	block := new(Block)
	if err := block.Deserialize(rawBlock); err != nil {
		return nil, fmt.Errorf("%s: deserialize block: %w", op, err)
	}

	if err := block.Verify(); err != nil {
		return nil, fmt.Errorf("%s: verify block: %w", op, err)
	}

	return block, nil
}

type WriteAheadLog struct {
	mux    sync.RWMutex
	head   int64
	driver driver.Driver
}

func NewWriteAheadLog(driver driver.Driver) *WriteAheadLog {
	if driver == nil {
		panic("wal/new: driver is nil")
	}

	return &WriteAheadLog{
		head:   0,
		driver: driver,
	}
}

func (wal *WriteAheadLog) Write(b []byte) (Reference, error) {
	wal.mux.Lock()
	defer wal.mux.Unlock()

	const op = "wal/write"

	if len(b) == 0 {
		return Reference{}, fmt.Errorf("%s: %w", op, ErrWrongBytes)
	}

	written, err := wal.driver.WriteAt(b, wal.head)
	if err != nil {
		return Reference{}, fmt.Errorf("%s: %w", op, err)
	}

	ref := Reference{
		Offset: wal.head,
		Size:   written,
	}

	wal.head += int64(written)

	return ref, nil
}

func (wal *WriteAheadLog) Read(ref Reference) ([]byte, error) {
	wal.mux.RLock()
	defer wal.mux.RUnlock()

	buf := make([]byte, ref.Size)

	if _, err := wal.driver.ReadAt(buf, ref.Offset); err != nil {
		return nil, fmt.Errorf("wal/read: %w", err)
	}

	return buf, nil
}
