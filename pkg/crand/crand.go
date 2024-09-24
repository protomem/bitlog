package crand

import (
	"math/rand"
	rand2 "math/rand/v2"
	"time"
	"unsafe"
)

const (
	_letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	_letterIdxBits = 6                     // 6 bits to represent a letter index
	_letterIdxMask = 1<<_letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	_letterIdxMax  = 63 / _letterIdxBits   // # of letter indices fitting in 63 bits
)

var _src = rand.NewSource(time.Now().UnixNano())

func Bytes(n int) []byte {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, _src.Int63(), _letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = _src.Int63(), _letterIdxMax
		}
		if idx := int(cache & _letterIdxMask); idx < len(_letterBytes) {
			b[i] = _letterBytes[idx]
			i--
		}
		cache >>= _letterIdxBits
		remain--
	}

	return b
}

func String(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, _src.Int63(), _letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = _src.Int63(), _letterIdxMax
		}
		if idx := int(cache & _letterIdxMask); idx < len(_letterBytes) {
			b[i] = _letterBytes[idx]
			i--
		}
		cache >>= _letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func Range(begin, end int64) int64 {
	return rand2.Int64N(end-begin) + begin
}
