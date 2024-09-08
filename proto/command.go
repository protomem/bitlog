package proto

import (
	"bytes"
	"errors"
)

type Command int

const (
	UNKNOWN Command = iota
	PING
	KEYS
	GET
	SET
	DEL
)

func ParseCommand(b []byte) (Command, []string, error) {
	tokens := bytes.Split(b, []byte(" "))
	if len(tokens) == 0 {
		return UNKNOWN, nil, NewErrUnknownCommand(string(b))
	}

	rawCmd := tokens[0]
	rawArgs := tokens[1:]

	var cmd Command
	cmd.UnmarshalText(rawCmd)

	args := Bytes2Strings(rawArgs...)
	if err := cmd.ValidateArgs(args...); err != nil {
		if errors.Is(err, ErrWrongArgs) {
			return cmd, args, NewErrWrongArgs(cmd.String())
		}

		return cmd, args, NewErrUnknownCommand(string(rawCmd), args...)
	}

	return cmd, args, nil
}

func (cmd *Command) UnmarshalText(b []byte) error {
	switch string(b) {
	case PING.String():
		*cmd = PING
	case KEYS.String():
		*cmd = KEYS
	case GET.String():
		*cmd = GET
	case SET.String():
		*cmd = SET
	case DEL.String():
		*cmd = DEL
	default:
		*cmd = UNKNOWN
	}

	if *cmd == UNKNOWN {
		return ErrUnknownCommand
	}
	return nil
}

func (cmd Command) String() string {
	switch cmd {
	case PING:
		return "PING"
	case KEYS:
		return "KEYS"
	case GET:
		return "GET"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	case UNKNOWN:
		fallthrough
	default:
		return "UNKNOWN"
	}
}

func (cmd Command) ValidateArgs(args ...string) error {
	var err error
	switch cmd {
	case PING:
		if len(args) > 1 {
			err = ErrWrongArgs
		}
	case KEYS:
		if len(args) != 1 {
			err = ErrWrongArgs
		}
	case GET:
		if len(args) != 1 {
			err = ErrWrongArgs
		}
	case SET:
		if len(args) != 2 {
			err = ErrWrongArgs
		}
	case DEL:
		if len(args) != 1 {
			err = ErrWrongArgs
		}
	case UNKNOWN:
		fallthrough
	default:
		err = ErrUnknownCommand
	}
	return err
}

func Bytes2Strings(bs ...[]byte) []string {
	ss := make([]string, 0, len(bs))
	for i := 0; i < len(bs); i++ {
		ss = append(ss, string(bs[i]))
	}
	return ss
}
