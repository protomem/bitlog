package binlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc64"
	"io"
	"sync"

	"github.com/protomem/bitlog/pkg/werrors"
)

const _journalErrorMsg = "binlog/journal"

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
	var off int
	data := make([]byte, 8+8+8+4+4+len(l.Key)+len(l.Value))

	// Encode header
	binary.LittleEndian.PutUint64(data[off:], uint64(l.Signature))
	off += 8

	binary.LittleEndian.PutUint64(data[off:], uint64(l.Timestamp))
	off += 8

	binary.LittleEndian.PutUint64(data[off:], uint64(l.Flags))
	off += 8

	binary.LittleEndian.PutUint32(data[off:], uint32(len(l.Key)))
	off += 4

	binary.LittleEndian.PutUint32(data[off:], uint32(len(l.Value)))
	off += 4

	// Encode body
	off += copy(data[off:], l.Key)
	copy(data[off:], l.Value)

	return data
}

func (l KeyValueLog) EncodeUnsign() []byte {
	var off int
	data := make([]byte, 8+8+4+4+len(l.Key)+len(l.Value))

	// Encode header
	binary.LittleEndian.PutUint64(data[off:], uint64(l.Timestamp))
	off += 8

	binary.LittleEndian.PutUint64(data[off:], uint64(l.Flags))
	off += 8

	binary.LittleEndian.PutUint32(data[off:], uint32(len(l.Key)))
	off += 4

	binary.LittleEndian.PutUint32(data[off:], uint32(len(l.Value)))
	off += 4

	// Encode body
	off += copy(data[off:], l.Key)
	copy(data[off:], l.Value)

	return data
}

func (l *KeyValueLog) Decode(data []byte) error {
	var off int
	if len(data) < 8+8+8+4+4 {
		return ErrSmallData
	}

	// Decode header
	l.Signature = binary.LittleEndian.Uint64(data[off:])
	off += 8

	l.Timestamp = int64(binary.LittleEndian.Uint64(data[off:]))
	off += 8

	l.Flags = binary.LittleEndian.Uint64(data[off:])
	off += 8

	keySize := binary.LittleEndian.Uint32(data[off:])
	off += 4

	valueSize := binary.LittleEndian.Uint32(data[off:])
	off += 4

	// Decode body
	if len(data) < off+int(keySize)+int(valueSize) {
		return ErrSmallData
	}

	l.Key = make([]byte, keySize)
	l.Value = make([]byte, valueSize)

	off += copy(l.Key, data[off:])
	copy(l.Value, data[off:])

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

type Driver interface {
	io.WriterAt
	io.ReaderAt
}

type Journal struct {
	driver Driver

	writeLock sync.Mutex
	headOff   int64
}

func NewJournal(driver Driver) *Journal {
	if driver == nil {
		werrors.PanicMessage(_journalErrorMsg, "driver is nil")
	}

	return &Journal{
		driver: driver,
	}
}

func (j *Journal) Write(log KeyValueLog) (LogID, error) {
	j.writeLock.Lock()
	defer j.writeLock.Unlock()

	log.Sign()

	var (
		data   = log.Encode()
		curOff = j.headOff
	)

	written, err := j.driver.WriteAt(data, curOff)
	if err != nil {
		return LogID{}, werrors.Error(err, _journalErrorMsg, "write")
	}

	j.headOff += int64(written)

	return LogID{Offset: curOff, Size: written}, nil
}

func (j *Journal) Read(lid LogID) (KeyValueLog, error) {
	data := make([]byte, lid.Size)
	_, err := j.driver.ReadAt(data, lid.Offset)
	if err != nil {
		return KeyValueLog{}, werrors.Error(err, _journalErrorMsg, "read")
	}

	var log KeyValueLog
	err = log.Decode(data)
	if err != nil {
		return KeyValueLog{}, werrors.Error(err, _journalErrorMsg, "decode")
	}

	if !log.Verify() {
		return KeyValueLog{}, werrors.Error(ErrInvalidLog, _journalErrorMsg, "verify")
	}

	return log, nil
}
