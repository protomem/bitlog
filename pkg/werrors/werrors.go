package werrors

import (
	"errors"
	"fmt"
	"strings"
)

func Wrap(msgs ...string) func(error, ...string) error {
	return func(err error, moreMsgs ...string) error {
		if err == nil {
			return nil
		}

		msgs = append(msgs, moreMsgs...)

		return Error(err, msgs...)
	}
}

func Error(err error, msgs ...string) error {
	if err == nil {
		return nil
	}

	msg := strings.Join(msgs, ": ")

	return fmt.Errorf("%s: %w", msg, err)
}

func Filter(err error, errs ...error) error {
	for _, filter := range errs {
		if errors.Is(err, filter) {
			return nil
		}
	}
	return err
}

func Ignore[T any](value T, _ error) T {
	return value
}

func Raise[T any](_ T, err error) error {
	return err
}
