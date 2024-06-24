package protocol

import (
	"errors"
	"fmt"
	"strings"
)

type Command int

var ErrUnknownCommand = fmt.Errorf("unknown command")

func NewErrUnknownCommand(cmd string, args ...string) error {
	return fmt.Errorf("%w '%s', with args beginning with: %s", ErrUnknownCommand, cmd, fmtArgs(args...))
}

const (
	_ Command = iota
)

func ParseCommand(s string) (Command, error) {
	switch {
	default:
		return 0, ErrUnknownCommand
	}
}

func (cmd Command) String() string {
	switch cmd {
	default:
		panic(ErrUnknownCommand)
	}
}

type Operation struct {
	Cmd  Command
	Args []string
}

func ParseOperation(cmdRaw string, args ...string) (Operation, error) {
	cmd, err := ParseCommand(cmdRaw)
	if err != nil {
		if errors.Is(err, ErrUnknownCommand) {
			return Operation{}, NewErrUnknownCommand(cmdRaw, args...)
		}

		return Operation{}, err
	}

	// TODO: validate args

	op := Operation{
		Cmd:  cmd,
		Args: args,
	}

	return op, nil
}

func fmtArgs(args ...string) string {
	fargs := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		fargs[i] = quote(args[i])
	}
	return strings.Join(fargs, " ")
}

func quote(s string) string {
	return "'" + s + "'"
}
