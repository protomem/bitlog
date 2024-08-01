package bitlog

import "context"

var _ Storage = (*DB)(nil)

type DB struct{}

type OpenOptions struct{}

func Open(path string, opts OpenOptions) (*DB, error) {
	return &DB{}, nil
}

func (db *DB) Keys(ctx context.Context) ([]Key, error) {
	panic("unimplemented")
}

func (db *DB) Find(ctx context.Context, pattern Key) ([]Key, error) {
	panic("unimplemented")
}

func (db *DB) Get(ctx context.Context, key Key) (Value, error) {
	panic("unimplemented")
}

func (db *DB) Set(ctx context.Context, key Key, value Value) error {
	panic("unimplemented")
}

func (db *DB) Del(ctx context.Context, key Key) error {
	panic("unimplemented")
}

func (db *DB) Merge(ctx context.Context) error {
	panic("unimplemented")
}

func (db *DB) Sync(ctx context.Context) error {
	panic("unimplemented")
}

func (db *DB) Check(ctx context.Context) error {
	panic("unimplemented")
}

func (db *DB) Close(ctx context.Context) error {
	panic("unimplemented")
}
