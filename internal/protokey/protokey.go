package protokey

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type ProtocolError struct {
	Message string
}

func (e *ProtocolError) Error() string {
	return "protocol error: " + e.Message
}

func IsProtocolError(err error) bool {
	var pErr *ProtocolError
	return errors.As(err, &pErr)
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(reader *bufio.Reader) *Parser {
	return &Parser{reader: reader}
}

func (p *Parser) ReadCommand() ([][]byte, error) {
	prefix, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '*' {
		return nil, &ProtocolError{Message: "expected array"}
	}

	arrayLenRaw, err := p.readLine()
	if err != nil {
		return nil, err
	}

	arrayLen, err := strconv.Atoi(arrayLenRaw)
	if err != nil || arrayLen <= 0 {
		return nil, &ProtocolError{Message: "invalid array length"}
	}

	args := make([][]byte, arrayLen)
	for i := range arrayLen {
		bulkPrefix, readErr := p.reader.ReadByte()
		if readErr != nil {
			return nil, normalizeReadErr(readErr, "failed read bulk prefix")
		}
		if bulkPrefix != '$' {
			return nil, &ProtocolError{Message: "expected bulk string"}
		}

		bulkLenRaw, readErr := p.readLine()
		if readErr != nil {
			return nil, readErr
		}

		bulkLen, convErr := strconv.Atoi(bulkLenRaw)
		if convErr != nil || bulkLen < 0 {
			return nil, &ProtocolError{Message: "invalid bulk string length"}
		}

		bulkData := make([]byte, bulkLen+2)
		if _, readErr = io.ReadFull(p.reader, bulkData); readErr != nil {
			return nil, normalizeReadErr(readErr, "failed read bulk data")
		}

		if bulkData[bulkLen] != '\r' || bulkData[bulkLen+1] != '\n' {
			return nil, &ProtocolError{Message: "invalid bulk string terminator"}
		}

		args[i] = bulkData[:bulkLen]
	}

	return args, nil
}

func (p *Parser) readLine() (string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", normalizeReadErr(err, "failed read line")
	}
	if len(line) < 2 || !strings.HasSuffix(line, "\r\n") {
		return "", &ProtocolError{Message: "invalid line terminator"}
	}

	return strings.TrimSuffix(line, "\r\n"), nil
}

func normalizeReadErr(err error, msg string) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &ProtocolError{Message: msg}
	}
	return err
}

type Writer struct {
	writer *bufio.Writer
}

func NewWriter(writer *bufio.Writer) *Writer {
	return &Writer{writer: writer}
}

func (w *Writer) Flush() error {
	return w.writer.Flush()
}

func (w *Writer) SimpleString(value string) error {
	_, err := fmt.Fprintf(w.writer, "+%s\r\n", value)
	return err
}

func (w *Writer) Error(message string) error {
	_, err := fmt.Fprintf(w.writer, "-%s\r\n", message)
	return err
}

func (w *Writer) BulkString(value []byte) error {
	if _, err := fmt.Fprintf(w.writer, "$%d\r\n", len(value)); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}
	_, err := w.writer.WriteString("\r\n")
	return err
}

func (w *Writer) NullBulkString() error {
	_, err := w.writer.WriteString("$-1\r\n")
	return err
}

func (w *Writer) Integer(value int64) error {
	_, err := fmt.Fprintf(w.writer, ":%d\r\n", value)
	return err
}

type HandlerFunc func(args [][]byte, writer *Writer) error

type Router struct {
	handlers map[string]HandlerFunc
}

func NewRouter() *Router {
	return &Router{handlers: make(map[string]HandlerFunc)}
}

func (r *Router) Handle(command string, handler HandlerFunc) {
	r.handlers[strings.ToUpper(command)] = handler
}

func (r *Router) Serve(command [][]byte, writer *Writer) error {
	if len(command) == 0 {
		return writer.Error("ERR empty command")
	}

	name := strings.ToUpper(string(command[0]))
	handler, exists := r.handlers[name]
	if !exists {
		return writer.Error("ERR unknown command")
	}

	return handler(command[1:], writer)
}
