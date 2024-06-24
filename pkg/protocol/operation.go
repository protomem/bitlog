package protocol

import (
	"errors"
	"strings"
)

type Command int

const (
	_ Command = iota
	PING
	GET
	SET
	DEL
)

func ParseCommand(s string) (Command, error) {
	switch {
	case strings.EqualFold(s, "ping"):
		return PING, nil
	case strings.EqualFold(s, "get"):
		return GET, nil
	case strings.EqualFold(s, "set"):
		return SET, nil
	case strings.EqualFold(s, "del"):
		return DEL, nil
	default:
		return 0, ErrUnknownCommand
	}
}

func (cmd Command) String() string {
	switch cmd {
	case PING:
		return "PING"
	case GET:
		return "GET"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
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
		if err := validatePingArgs(args...); err != nil {
			return Operation{}, err
		}
	case GET:
		if err := validateGetArgs(args...); err != nil {
			return Operation{}, err
		}
	case SET:
		if err := validateSetArgs(args...); err != nil {
			return Operation{}, err
		}
	case DEL:
		if err := validateDelArgs(args...); err != nil {
			return Operation{}, err
		}
	}

	op := Operation{
		Cmd:  cmd,
		Args: args,
	}

	return op, nil
}

func validatePingArgs(args ...string) error {
	if len(args) > 1 {
		return NewWrongNumberOfArguments(PING)
	}
	return nil
}

func validateGetArgs(args ...string) error {
	if len(args) != 1 {
		return NewWrongNumberOfArguments(GET)
	}
	return nil
}

func validateSetArgs(args ...string) error {
	if len(args) != 2 {
		return NewWrongNumberOfArguments(SET)
	}
	return nil
}

func validateDelArgs(args ...string) error {
	if len(args) == 0 {
		return NewWrongNumberOfArguments(DEL)
	}
	return nil
}
