package crand

import (
	"math/rand"
	"strconv"
	"time"
	"unsafe"
)

const (
	Letter = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	Number = "0123456789"

	AlphaNum = Letter + Number
)

const (
	_letterIdxBits = 6                     // 6 bits to represent a letter index
	_letterIdxMask = 1<<_letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	_letterIdxMax  = 63 / _letterIdxBits   // # of letter indices fitting in 63 bits
)

func Gen(n int, alphabet string) string {
	src := rand.NewSource(time.Now().UnixNano())
	b := make([]byte, n)

	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), _letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), _letterIdxMax
		}
		if idx := int(cache & _letterIdxMask); idx < len(alphabet) {
			b[i] = alphabet[idx]
			i--
		}
		cache >>= _letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func GenInt64(n int) int64 {
	val := Gen(n, Number)
	parseVal, _ := strconv.ParseInt(val, 10, 64)
	return parseVal
}
