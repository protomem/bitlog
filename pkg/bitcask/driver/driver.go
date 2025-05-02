package driver

import (
	"io"
)

type DriverFactory interface {
	Driver(name string) (Driver, error)
}

type Driver interface {
	Name() string

	io.WriterAt
	io.ReaderAt
}
