package werrors

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const _msgSeparator = ": "

func Error(err error, msg ...string) error {
	if err == nil {
		return nil
	}

	fullMsg := buildErrMsg(msg...)
	if fullMsg == "" {
		return err
	}

	return fmt.Errorf("%s%s%w", fullMsg, _msgSeparator, err)
}

func ErrorMessage(msg ...string) error {
	fullMsg := buildErrMsg(msg...)
	if fullMsg == "" {
		return nil
	}

	return errors.New(fullMsg)
}

func Panic(err error, msg ...string) {
	panicErr := Error(err, msg...)
	if panicErr == nil {
		return
	}

	panic(panicErr)
}

func PanicMessage(msg ...string) {
	panicMsg := buildErrMsg(msg...)
	if panicMsg == "" {
		return
	}

	panic(panicMsg)
}

func buildErrMsg(msg ...string) string {
	msg = slices.DeleteFunc(msg, func(msg string) bool {
		return strings.TrimSpace(msg) == ""
	})

	if len(msg) == 0 {
		return ""
	}

	return strings.Join(msg, _msgSeparator)
}
