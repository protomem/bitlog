package proto

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrUnknownCommand = errors.New("unknown command")
	ErrWrongArgs      = errors.New("wrong number of arguments")
)

func NewErrUnknownCommand(cmd string, args ...string) error {
	return fmt.Errorf("%w '%s', with args beginning with: %s", ErrUnknownCommand, cmd, sliceToString(args))
}

func NewErrWrongArgs(cmd string) error {
	return fmt.Errorf("%w for '%s' command", ErrWrongArgs, cmd)
}

func sliceToString(ss []string) string {
	return strings.Join(ss, ", ")
}
