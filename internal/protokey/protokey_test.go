package protokey_test

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/protomem/bitlog/internal/protokey"
)

func TestParser_ReadCommandLoop(t *testing.T) {
	input := "" +
		"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n" +
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"

	parser := protokey.NewParser(bufio.NewReader(strings.NewReader(input)))

	cmd, err := parser.ReadCommand()
	if err != nil {
		t.Fatalf("first read failed with err=%v", err)
	}
	if len(cmd) != 2 || string(cmd[0]) != "PING" || string(cmd[1]) != "hello" {
		t.Fatalf("invalid first command=%q", cmd)
	}

	cmd, err = parser.ReadCommand()
	if err != nil {
		t.Fatalf("second read failed with err=%v", err)
	}
	if len(cmd) != 3 || string(cmd[0]) != "SET" || string(cmd[1]) != "key" || string(cmd[2]) != "value" {
		t.Fatalf("invalid second command=%q", cmd)
	}

	_, err = parser.ReadCommand()
	if err != io.EOF {
		t.Fatalf("expected io.EOF, actual=%v", err)
	}
}

func TestParser_Malformed(t *testing.T) {
	cases := []string{
		"+OK\r\n",
		"*1\r\n+PING\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$3\r\npi\r\n",
	}

	for i, tc := range cases {
		parser := protokey.NewParser(bufio.NewReader(strings.NewReader(tc)))
		_, err := parser.ReadCommand()
		if err == nil {
			t.Fatalf("case[%d]: expected error", i)
		}
		if !protokey.IsProtocolError(err) {
			t.Fatalf("case[%d]: expected protocol error, actual=%v", i, err)
		}
	}
}

func TestRouter_DispatchAndUnknown(t *testing.T) {
	router := protokey.NewRouter()
	router.Handle("PING", func(args [][]byte, writer *protokey.Writer) error {
		if len(args) != 1 {
			return writer.Error("ERR invalid args")
		}
		return writer.BulkString(args[0])
	})

	var out bytes.Buffer
	writer := protokey.NewWriter(bufio.NewWriter(&out))

	if err := router.Serve([][]byte{[]byte("pInG"), []byte("hello")}, writer); err != nil {
		t.Fatalf("serve ping failed with err=%v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush failed with err=%v", err)
	}

	if out.String() != "$5\r\nhello\r\n" {
		t.Fatalf("invalid response=%q", out.String())
	}

	out.Reset()
	writer = protokey.NewWriter(bufio.NewWriter(&out))
	if err := router.Serve([][]byte{[]byte("UNKNOWN")}, writer); err != nil {
		t.Fatalf("serve unknown failed with err=%v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush unknown failed with err=%v", err)
	}

	if out.String() != "-ERR unknown command\r\n" {
		t.Fatalf("invalid unknown response=%q", out.String())
	}
}
