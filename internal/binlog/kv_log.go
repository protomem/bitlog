package binlog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"hash/crc64"
	"io"
	"time"

	"github.com/protomem/bitlog/pkg/bin"
)

type KeyValueJournal = Journal[*KeyValueLog]

func NewKeyValueJournal(driver Driver) *KeyValueJournal {
	return NewJournal(driver, NewLogBuilder(EmptyKeyValueLog))
}

type KeyValueLog struct {
	Signature uint64 // CRC 64

	Timestamp int64 // UNIX Timestamp

	// Bitset
	Flags uint64

	Key   []byte
	Value []byte
}

func EmptyKeyValueLog() *KeyValueLog {
	return &KeyValueLog{}
}

func NewKeyValueLog(tstamp int64, key, value []byte) *KeyValueLog {
	return &KeyValueLog{
		Timestamp: tstamp,
		Key:       key,
		Value:     value,
	}
}

func NowKeyValueLog(key, value []byte) *KeyValueLog {
	return NewKeyValueLog(time.Now().UnixMilli(), key, value)
}

func TombstoneKeyValueLog(key []byte) *KeyValueLog {
	return NowKeyValueLog(key, nil)
}

func (l *KeyValueLog) WithDeleted() *KeyValueLog {
	l.Flags |= 1 << 0
	return l
}

func (l *KeyValueLog) Sign() {
	l.Signature = l.GenSign()
}

func (l *KeyValueLog) Verify() bool {
	return l.Signature == l.GenSign()
}

func (l *KeyValueLog) GenSign() uint64 {
	keySign := crc32.Checksum(l.Key, crc32.IEEETable)
	valueSign := crc32.Checksum(l.Value, crc32.IEEETable)

	size := l.sizeMeta() + bin.SizeOfValue(keySign) + bin.SizeOfValue(valueSign)
	data := make([]byte, 0, size)

	data = bin.Append(binary.LittleEndian, data, uint64(l.Timestamp))
	data = bin.Append(binary.LittleEndian, data, uint64(l.Flags))
	data = bin.Append(binary.LittleEndian, data, uint32(len(l.Key)))
	data = bin.Append(binary.LittleEndian, data, uint32(len(l.Value)))

	data = bin.Append(binary.LittleEndian, data, keySign)
	data = bin.Append(binary.LittleEndian, data, valueSign)

	return crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
}

func (l *KeyValueLog) Encode(dest io.Writer) (written int, err error) {
	headData := make([]byte, 0, l.sizeHead())

	headData = bin.Append(binary.LittleEndian, headData, l.Signature)
	headData = bin.Append(binary.LittleEndian, headData, uint64(l.Timestamp))
	headData = bin.Append(binary.LittleEndian, headData, l.Flags)

	headData = bin.Append(binary.LittleEndian, headData, uint32(len(l.Key)))
	headData = bin.Append(binary.LittleEndian, headData, uint32(len(l.Value)))

	writeHead, err := dest.Write(headData)
	written += writeHead
	if err != nil {
		return
	}

	writeKey, err := dest.Write(l.Key)
	written += writeKey
	if err != nil {
		return
	}

	writeValue, err := dest.Write(l.Value)
	written += writeValue

	return
}

func (l *KeyValueLog) Decode(src io.Reader) (read int, err error) {
	headData := make([]byte, l.sizeHead())

	readHead, err := src.Read(headData)
	read += readHead
	if err != nil {
		return
	}
	if readHead < len(headData) {
		return read, ErrUnexpectedSize
	}

	var headOff int

	headOff += bin.ValueTo(binary.LittleEndian, headData[headOff:], &l.Signature)

	var tstamp uint64
	headOff += bin.ValueTo(binary.LittleEndian, headData[headOff:], &tstamp)
	l.Timestamp = int64(tstamp)

	headOff += bin.ValueTo(binary.LittleEndian, headData[headOff:], &l.Flags)

	var keySize, valueSize uint32
	headOff += bin.ValueTo(binary.LittleEndian, headData[headOff:], &keySize)
	headOff += bin.ValueTo(binary.LittleEndian, headData[headOff:], &valueSize)

	bodyData := make([]byte, keySize+valueSize)

	l.Key = bodyData[:keySize]
	l.Value = bodyData[keySize:]

	readBody, err := src.Read(bodyData)
	read += readBody

	return
}

func (l *KeyValueLog) Size() int {
	return l.sizeHead() + l.sizeBody()
}

func (l *KeyValueLog) Equals(other *KeyValueLog) bool {
	if l == other {
		return true
	}
	if other == nil {
		return false
	}

	return l.Signature == other.Signature &&
		l.Timestamp == other.Timestamp && l.Flags == other.Flags &&
		bytes.Equal(l.Key, other.Key) && bytes.Equal(l.Value, other.Value)
}

func (l *KeyValueLog) sizeHead() int {
	return bin.SizeOfValue(l.Signature) + l.sizeMeta()
}

func (l *KeyValueLog) sizeMeta() int {
	return bin.SizeOfValue(l.Timestamp) + bin.SizeOfValue(l.Flags) +
		bin.SizeOfValue(uint32(len(l.Key))) + bin.SizeOfValue(uint32(len(l.Value)))
}

func (l *KeyValueLog) sizeBody() int {
	return len(l.Key) + len(l.Value)
}
