package bitlog

import "context"

type (
	Key   = []byte
	Value = []byte
)

type Storage interface {
	Keys(ctx context.Context) ([]Key, error)
	Find(ctx context.Context, pattern Key) ([]Key, error)

	Get(ctx context.Context, key Key) (Value, error)
	Set(ctx context.Context, key Key, value Value) error
	Del(ctx context.Context, key Key) error

	// Merges old files and creates hints.
	Merge(ctx context.Context) error

	// Creates a new active file.
	Sync(ctx context.Context) error

	// Sends a PING signal to the server.
	Check(ctx context.Context) error

	Close(ctx context.Context) error
}
