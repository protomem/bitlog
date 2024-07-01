package protocol

import (
	"errors"
	"strings"
)

type Command int

const (
	UNKNOWN Command = iota
	PING
	GET
	SET
	DEL
	KEYS
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
	case strings.EqualFold(s, "keys"):
		return KEYS, nil

	case strings.EqualFold(s, "unknown"):
		fallthrough
	default:
		return UNKNOWN, ErrUnknownCommand
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
	case KEYS:
		return "KEYS"

	default:
		return "UNKNOWN"
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
	case KEYS:
		if err := validateKeysArgs(args...); err != nil {
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
		return NewErrWrongNumberOfArguments(PING)
	}
	return nil
}

func validateGetArgs(args ...string) error {
	if len(args) != 1 {
		return NewErrWrongNumberOfArguments(GET)
	}
	return nil
}

func validateSetArgs(args ...string) error {
	if len(args) != 2 {
		return NewErrWrongNumberOfArguments(SET)
	}
	return nil
}

func validateDelArgs(args ...string) error {
	if len(args) == 0 {
		return NewErrWrongNumberOfArguments(DEL)
	}
	return nil
}

func validateKeysArgs(args ...string) error {
	if len(args) != 1 {
		return NewErrWrongNumberOfArguments(KEYS)
	}
	return nil
}
