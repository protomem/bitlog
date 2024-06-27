package bitcask

type DB struct{}

func Open() (*DB, error) {
	return &DB{}, nil
}

func (db *DB) Keys() ([][]byte, error) {
	panic("unimplemented")
}

func (db *DB) Range(iterator func(key []byte, value []byte) bool) error {
	panic("unimplemented")
}

func (db *DB) Has(key []byte) (bool, error) {
	panic("unimplemented")
}

func (db *DB) Get(key []byte) ([]byte, error) {
	panic("unimplemented")
}

func (db *DB) Set(key []byte, value []byte) error {
	panic("unimplemented")
}

func (db *DB) Delete(key []byte) error {
	panic("unimplemented")
}

func (db *DB) Merge(key []byte, value []byte) error {
	panic("unimplemented")
}

func (db *DB) Close() error {
	panic("unimplemented")
}
