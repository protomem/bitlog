package bitcask

type DB struct{}

func Open(path string) (*DB, error) {
	return &DB{}, nil
}

func (db *DB) Close() error {
	return nil
}
