package bitlog

import "context"

var _ Storage = (*DB)(nil)

type DB struct{}

func Open() (*DB, error) {
	return &DB{}, nil
}

func (db *DB) Keys(_ context.Context) ([][]byte, error) {
	panic("unimplemented")
}

func (db *DB) Range(_ context.Context, iterator func(key []byte, value []byte) bool) error {
	panic("unimplemented")
}

func (db *DB) Get(_ context.Context, key []byte) ([]byte, error) {
	panic("unimplemented")
}

func (db *DB) Set(_ context.Context, key []byte, value []byte) error {
	panic("unimplemented")
}

func (db *DB) Delete(_ context.Context, key []byte) error {
	panic("unimplemented")
}

func (db *DB) Merge(_ context.Context, key []byte, value []byte) error {
	panic("unimplemented")
}

func (db *DB) Close(_ context.Context) error {
	panic("unimplemented")
}
