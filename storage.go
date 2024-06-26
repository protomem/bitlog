package bitlog

import "context"

type Storage interface {
	Keys(ctx context.Context) ([][]byte, error)
	Range(ctx context.Context, iterator func(key, value []byte) bool) error

	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error

	Merge(ctx context.Context, key []byte, value []byte) error

	Close(ctx context.Context) error
}
