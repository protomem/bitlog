package bincode

import "encoding/binary"

type BinaryValueConstraint interface {
	uint | uint16 | uint32 | uint64
}

func Put[T BinaryValueConstraint](order binary.ByteOrder, dest []byte, value T) (bytes int) {
	switch v := any(value).(type) {
	case uint:
		if dest != nil {
			order.PutUint32(dest, uint32(v))
		}
		bytes = 4
	case uint16:
		if dest != nil {
			order.PutUint16(dest, uint16(v))
		}
		bytes = 2
	case uint32:
		if dest != nil {
			order.PutUint32(dest, uint32(v))
		}
		bytes = 4
	case uint64:
		if dest != nil {
			order.PutUint64(dest, uint64(v))
		}
		bytes = 8
	}
	return
}

func Value[T BinaryValueConstraint](order binary.ByteOrder, src []byte, value *T) int {
	var rawValue T
	switch any(rawValue).(type) {
	case uint:
		rawValue = T(order.Uint32(src))
		*value = rawValue
		return 4
	case uint16:
		rawValue = T(order.Uint16(src))
		*value = rawValue
		return 2
	case uint32:
		rawValue = T(order.Uint32(src))
		*value = rawValue
		return 4
	case uint64:
		rawValue = T(order.Uint64(src))
		*value = rawValue
		return 8
	default:
		return 0
	}
}

func SliceByOffset(src []byte, off int) []byte {
	if off < 0 || off > len(src) {
		return nil
	}
	return src[off:]
}
