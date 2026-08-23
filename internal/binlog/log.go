package binlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/protomem/bitlog/internal/bin"
)

var ErrUnexpectedSize = errors.New("unexpected size data")

type LogID struct {
	Offset int64
	Size   int
}

type Log interface {
	Sign()
	Verify() bool

	Size() int

	Encode(dest io.Writer) (written int, err error)
	Decode(src io.Reader) (read int, err error)
}

type KeyValueLog struct {
	Signature uint64 // CRC 64

	Timestamp int64  // UNIX Timestamp
	Flags     uint64 // Bitset

	Key   []byte
	Value []byte
}

func NewKeyValueLog(tstamp int64, key, value []byte) KeyValueLog {
	return KeyValueLog{
		Timestamp: tstamp,
		Key:       key,
		Value:     value,
	}
}

func (l *KeyValueLog) Sign() {
}

func (l *KeyValueLog) Verify() bool {
	// TODO
	return true
}

func (l *KeyValueLog) GenSign() uint64 {
	// TODO
	return 0
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

	l.Key = make([]byte, keySize)
	l.Value = make([]byte, valueSize)

	readKey, err := src.Read(l.Key)
	read += readKey
	if err != nil {
		return
	}

	readValue, err := src.Read(l.Value)
	read += readValue

	return
}

func (l *KeyValueLog) Size() int {
	return l.sizeHead() + l.sizeBody()
}

func (l *KeyValueLog) sizeHead() int {
	return bin.SizeOfValue(l.Signature) +
		bin.SizeOfValue(l.Timestamp) + bin.SizeOfValue(l.Flags) +
		bin.SizeOfValue(uint32(len(l.Key))) + bin.SizeOfValue(uint32(len(l.Value)))
}

func (l *KeyValueLog) sizeBody() int {
	return len(l.Key) + len(l.Value)
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
