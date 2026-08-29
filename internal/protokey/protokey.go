package protokey

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"github.com/protomem/bitlog/pkg/werrors"
)

const _pkgErrorMsg = "protokey"

var ErrInsufficientArgs = errors.New("insufficient arguments error")

type Command int

const (
	UNKOWN Command = iota
	PING
	GET
	SET
	DEL
)

type (
	ArgsKind int

	Args map[ArgsKind][]byte
)

const (
	_ ArgsKind = iota
	KeyKind
	ValueKind
	TTLKind
)

func Parse(src io.Reader) (Command, Args, error) {
	tokenizer := newTokenizer(src)

	var cmd Command
	args := defaultArgs()

	for i := 0; tokenizer.Scan(); i++ {
		token := tokenizer.Bytes()

		if i == 0 {
			cmd = selectCommand(token)
			continue
		}

		if i == int(KeyKind) || i == int(ValueKind) {
			args[ArgsKind(i)] = bytes.Clone(token)
		}
	}

	return cmd, args, nil
}

func Validate(cmd Command, args Args) error {
	switch cmd {
	case UNKOWN:
		fallthrough
	default:
		return nil

	case GET:
		if !checkArg(args, KeyKind) {
			return werrors.Error(ErrInsufficientArgs, _pkgErrorMsg)
		}

		return nil

	case SET:
		if checkArg(args, KeyKind) {
			return werrors.Error(ErrInsufficientArgs, _pkgErrorMsg)
		}
		if checkArg(args, ValueKind) {
			return werrors.Error(ErrInsufficientArgs, _pkgErrorMsg)
		}

		return nil

	case DEL:
		if checkArg(args, KeyKind) {
			return werrors.Error(ErrInsufficientArgs, _pkgErrorMsg)
		}

		return nil
	}
}

func newTokenizer(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	return scanner
}

func selectCommand(token []byte) Command {
	switch string(token) {
	default:
		return UNKOWN

	case "PING":
		return PING
	case "GET":
		return GET
	case "SET":
		return SET
	case "DEL":
		return DEL
	}
}

func defaultArgs() Args {
	return Args{
		KeyKind: nil,
	}
}

func checkArg(args Args, kind ArgsKind) bool {
	_, ok := args[kind]
	return ok
}
