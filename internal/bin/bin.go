package bin

import (
	"encoding/binary"
	"unsafe"
)

type Sizable interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~complex64 | ~complex128
}

func Size[T Sizable]() int {
	var v T
	return int(unsafe.Sizeof(v))
}

func SizeOfValue[T Sizable](_ T) int {
	return Size[T]()
}

type BinaryValue interface {
	~uint16 | ~uint32 | ~uint64
}

func Append[T BinaryValue](order binary.AppendByteOrder, dest []byte, value T) []byte {
	switch v := any(value).(type) {
	case uint16:
		return order.AppendUint16(dest, v)
	case uint32:
		return order.AppendUint32(dest, v)
	case uint64:
		return order.AppendUint64(dest, v)
	default:
		return dest
	}
}

func Value[T BinaryValue](order binary.ByteOrder, src []byte) T {
	var zero T
	switch any(zero).(type) {
	case uint16:
		return T(order.Uint16(src))
	case uint32:
		return T(order.Uint32(src))
	case uint64:
		return T(order.Uint64(src))
	default:
		return zero
	}
}

func ValueTo[T BinaryValue](order binary.ByteOrder, src []byte, value *T) int {
	*value = Value[T](order, src)
	return Size[T]()
}
