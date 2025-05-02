package driver

import (
	"io"
)

type Driver interface {
	Name() string

	io.WriterAt
	io.ReaderAt
}
