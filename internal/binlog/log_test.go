package binlog_test

import (
	"bytes"
	"testing"

	"github.com/protomem/bitlog/internal/binlog"
)

func TestKeyValueLog_Encoding(t *testing.T) {
	originLog := binlog.NewKeyValueLog(1_000_000, []byte("key"), []byte("value"))

	var encodedData bytes.Buffer
	if actualWritten, err := originLog.Encode(&encodedData); err != nil {
		t.Fatalf("failed encode log with error=%s", err)
	} else if expectedWritten := originLog.Size(); actualWritten != expectedWritten {
		t.Fatalf("failed encode log: actualWritten=%d not equal expectedWritten=%d", actualWritten, expectedWritten)
	}

	var decodedLog binlog.KeyValueLog
	if _, err := decodedLog.Decode(&encodedData); err != nil {
		t.Fatalf("failed decode log with error=%s", err)
	}

	if !originLog.Equals(&decodedLog) {
		t.Fatalf("logs not equals, originLog=%+v, decodeLog=%+v", originLog, decodedLog)
	}
}
