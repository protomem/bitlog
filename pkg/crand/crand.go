package crand

import "math/rand"

func Range(min, max int) int {
	return min + int(rand.Int63n(int64(max-min)))
}
