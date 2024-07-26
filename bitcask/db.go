package bitcask

type DB struct {
	index *keyDir
	files []*dataFile
}

func Open(path string) (*DB, error) {
	return &DB{
		index: newKeyDir(),
		files: []*dataFile{},
	}, nil
}

type keyDir struct{}

func newKeyDir() *keyDir {
	return &keyDir{}
}
