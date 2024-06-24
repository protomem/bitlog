package protocol

import (
	"errors"
	"strings"
)

type Command int

const (
	_ Command = iota
	PING
)

func ParseCommand(s string) (Command, error) {
	switch {
	case strings.EqualFold(s, "ping"):
		return PING, nil
	default:
		return 0, ErrUnknownCommand
	}
}

func (cmd Command) String() string {
	switch cmd {
	case PING:
		return "PING"
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

	switch cmd {
	case PING:
		if err := verifyPingArgs(args...); err != nil {
			return Operation{}, err
		}
	}

	op := Operation{
		Cmd:  cmd,
		Args: args,
	}

	return op, nil
}

func verifyPingArgs(args ...string) error {
	if len(args) > 1 {
		return NewWrongNumberOfArguments(PING)
	}
	return nil
}
