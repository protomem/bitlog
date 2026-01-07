package redisproto

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

var ErrInvalidOperation = errors.New("invalid operation")

type Operation int

const (
	OpGet Operation = iota
	OpSet
)

func (op Operation) String() string {
	switch op {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	Op   Operation
	Args [][]byte
}

func NewCommand(op Operation, args [][]byte) *Command {
	return &Command{
		Op:   op,
		Args: args,
	}
}

func CommandFromReader(src io.Reader) (*Command, error) {
	r := bufio.NewReader(src)
	scanner := bufio.NewScanner(r)

	// Split by space
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, ' '); i >= 0 {
			return i + 1, data[:i], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}

		return nil, io.ErrUnexpectedEOF
	}

	var op Operation
	rawOp := scanner.Text()

	switch rawOp {
	case "GET":
		op = OpGet
	case "SET":
		op = OpSet
	default:
		return nil, ErrInvalidOperation
	}

	opArgs := make([][]byte, 0)
	for scanner.Scan() {
		arg := scanner.Bytes()
		if len(arg) == 0 {
			continue
		}
		opArgs = append(opArgs, arg)
	}

	return NewCommand(op, opArgs), nil
}

func (cmd *Command) String() string {
	var sb strings.Builder
	sb.WriteString("'" + cmd.Op.String() + "'")
	sb.WriteString(": [")
	for i, arg := range cmd.Args {
		sb.WriteString(string(arg))
		if i < len(cmd.Args)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
