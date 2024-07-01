package protocol

import "fmt"

var (
	ErrUnknownCommand         = fmt.Errorf("unknown command")
	ErrWrongNumberOfArguments = fmt.Errorf("wrong number of arguments")
)

func NewErrUnknownCommand(cmd string, args ...string) error {
	return fmt.Errorf("%w '%s', with args beginning with: %s", ErrUnknownCommand, cmd, fmtArgs(args...))
}

func NewErrWrongNumberOfArguments(cmd Command) error {
	return fmt.Errorf("%w for %s command", ErrWrongNumberOfArguments, quote(cmd.String()))
}
