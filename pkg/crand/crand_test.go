package crand_test

import (
	"testing"

	"github.com/protomem/bitlog/pkg/crand"
)

func TestGenInt64(t *testing.T) {
	// TODO: test -> benchmark

	set := make(map[int64]struct{})
	for i := 0; i < 10000; i++ {
		val := crand.GenInt64(12)
		if _, ok := set[val]; ok {
			t.Fatalf("duplicate value: %d", val)
		}
		set[val] = struct{}{}
	}
}
