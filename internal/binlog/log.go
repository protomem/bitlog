package binlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"

	"github.com/protomem/bitlog/internal/bincode"
)

var (
	ErrInvalidLog = errors.New("invalid log")
	ErrSmallData  = errors.New("small data")
)

type LogID struct {
	Offset int64
	Size   int
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

func (l KeyValueLog) GenSign() uint64 {
	data := l.EncodeUnsign()
	sign := crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))

	return sign
}

func (l *KeyValueLog) Sign() {
	l.Signature = l.GenSign()
}

func (l KeyValueLog) Verify() bool {
	return l.Signature == l.GenSign()
}

func (l KeyValueLog) Encode() []byte {
	var (
		off  int
		data = make([]byte, l.encodeHeaderTo(nil)+l.encodeBodyTo(nil))
	)

	off += l.encodeHeaderTo(data)
	l.encodeBodyTo(bincode.SliceByOffset(data, off))

	return data
}

func (l KeyValueLog) EncodeUnsign() []byte {
	var (
		off  int
		data = make([]byte, l.encodeHeaderUnsignTo(nil)+l.encodeBodyTo(nil))
	)

	off += l.encodeHeaderUnsignTo(data)
	l.encodeBodyTo(bincode.SliceByOffset(data, off))

	return data
}

func (l *KeyValueLog) Decode(data []byte) error {
	headOff := l.decodeHeaderFrom(data)
	if headOff == 0 {
		return ErrSmallData
	}

	bodyOff := l.decodeBodyFrom(bincode.SliceByOffset(data, headOff))
	if bodyOff == 0 {
		return ErrSmallData
	}

	return nil
}

func (l KeyValueLog) Equals(other KeyValueLog) bool {
	return l.Signature == other.Signature &&
		l.Timestamp == other.Timestamp && l.Flags == other.Flags &&
		bytes.Equal(l.Key, other.Key) && bytes.Equal(l.Value, other.Value)
}

func (l KeyValueLog) Clone() KeyValueLog {
	newLog := l

	newLog.Key = bytes.Clone(l.Key)
	newLog.Value = bytes.Clone(l.Value)

	return newLog
}

func (l KeyValueLog) encodeHeaderTo(dest []byte) (off int) {
	off += bincode.Put(binary.LittleEndian, dest, l.Signature)
	off += l.encodeHeaderUnsignTo(bincode.SliceByOffset(dest, off))

	return off
}

func (l *KeyValueLog) decodeHeaderFrom(src []byte) (off int) {
	if len(src) < l.encodeHeaderTo(nil) {
		return 0
	}

	off += bincode.Value(binary.LittleEndian, bincode.SliceByOffset(src, off), &l.Signature)
	off += l.decodeHeaderUnsignFrom(bincode.SliceByOffset(src, off))

	return off
}

func (l KeyValueLog) encodeHeaderUnsignTo(dest []byte) (off int) {
	off += bincode.Put(binary.LittleEndian, bincode.SliceByOffset(dest, off), uint64(l.Timestamp))
	off += bincode.Put(binary.LittleEndian, bincode.SliceByOffset(dest, off), l.Flags)
	off += bincode.Put(binary.LittleEndian, bincode.SliceByOffset(dest, off), uint32(len(l.Key)))
	off += bincode.Put(binary.LittleEndian, bincode.SliceByOffset(dest, off), uint32(len(l.Value)))

	return off
}

func (l *KeyValueLog) decodeHeaderUnsignFrom(src []byte) (off int) {
	if len(src) < l.encodeHeaderUnsignTo(nil) {
		return 0
	}

	var ts uint64
	off += bincode.Value(binary.LittleEndian, bincode.SliceByOffset(src, off), &ts)
	l.Timestamp = int64(ts)

	off += bincode.Value(binary.LittleEndian, bincode.SliceByOffset(src, off), &l.Flags)

	var keySize, valueSize uint32
	off += bincode.Value(binary.LittleEndian, bincode.SliceByOffset(src, off), &keySize)
	off += bincode.Value(binary.LittleEndian, bincode.SliceByOffset(src, off), &valueSize)

	l.Key = make([]byte, keySize)
	l.Value = make([]byte, valueSize)

	return off
}

func (l KeyValueLog) encodeBodyTo(data []byte) (off int) {
	if data == nil {
		return len(l.Key) + len(l.Value)
	}

	off += copy(data[off:], l.Key)
	off += copy(data[off:], l.Value)

	return off
}

func (l *KeyValueLog) decodeBodyFrom(src []byte) (off int) {
	if len(src) < len(l.Key)+len(l.Value) {
		return 0
	}

	off += copy(l.Key, src[off:])
	off += copy(l.Value, src[off:])

	return off
}
