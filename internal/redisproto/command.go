package redisproto

import "io"

type Type int

type Command struct {
	Type Type
	Args [][]byte
}

func NewCommand(t Type, args ...[]byte) *Command {
	return &Command{
		Type: t,
		Args: args,
	}
}

func NewCommandFromReader(r io.Reader) (*Command, error) {
	return NewCommand(0, nil), nil
}
