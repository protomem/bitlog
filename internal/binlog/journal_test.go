package binlog_test

import (
	"testing"

	"github.com/protomem/bitlog/internal/binlog"
)

func Test_KeyValueLog_Encoding(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		originLog := binlog.NewKeyValueLog(1_000_000_1, []byte("key"), []byte("value"))
		encodedData := originLog.Encode()

		var decodedLog binlog.KeyValueLog
		if err := decodedLog.Decode(encodedData); err != nil {
			t.Fatalf("failed decode log: %s", err)
		}

		if !decodedLog.Equals(originLog) {
			t.Fatalf("failed compare logs: originLog(%+v) and decodedLog(%+v)", originLog, decodedLog)
		}
	})
}

func Test_KeyValueLog_Verification(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		originLog := binlog.NewKeyValueLog(1_000_000_1, []byte("key"), []byte("value"))
		originLog.Sign()

		if !originLog.Verify() {
			t.Fatalf("failed verify log")
		}
	})
}

func Test_Journal_WriteRead(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// TODO
	})
}
