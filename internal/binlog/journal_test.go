package binlog_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		file := openTestFile(t, "journal.binlog")
		journal := binlog.NewJournal(file)

		var writtenLogs []binlog.LogID
		for i := 1; i <= 10; i++ {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)

			log := binlog.NewKeyValueLog(nowTimeUnix(), []byte(key), []byte(value))

			if lid, err := journal.Write(log); err == nil {
				writtenLogs = append(writtenLogs, lid)
			} else {
				t.Fatalf("failed write log(%+v): %s", log, err)
			}
		}

		for _, lid := range writtenLogs {
			if log, err := journal.Read(lid); err == nil {
				t.Logf("read log %+v", log)
			} else {
				t.Fatalf("failed read log by id(%+v): %s", lid, err)
			}
		}
	})
}

func nowTimeUnix() int64 {
	return time.Now().Unix()
}

func openTestFile(t *testing.T, name string) *os.File {
	t.Helper()

	fullname := filepath.Join(t.TempDir(), name)

	file, err := os.OpenFile(fullname, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		t.Fatalf("fialed open file(%s): %s", fullname, err)
	}

	return file
}
