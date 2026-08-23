package binlog_test

import (
	"encoding/base32"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/protomem/bitlog/internal/binlog"
)

func TestKeyValueJournal_WriteRead(t *testing.T) {
	driver := openTestFile(t, "journal.binlog")
	journal := binlog.NewKeyValueJournal(driver)

	logs := genKeyValueLogs(t, 100)
	logIDs := make([]binlog.LogID, len(logs))
	expectedOff := int64(0)

	for i, log := range logs {
		logID, err := journal.Write(log)
		if err != nil {
			t.Fatalf("failed write log[%d] with err=%s", i, err)
		}

		if logID.Offset != expectedOff {
			t.Fatalf("invalid log offset: actual=%d expected=%d", logID.Offset, expectedOff)
		}
		if logID.Size != log.Size() {
			t.Fatalf("invalid log size: actual=%d expected=%d", logID.Size, log.Size())
		}

		expectedOff += int64(log.Size())
		logIDs[i] = logID
	}

	for i, logID := range logIDs {
		actualLog, err := journal.Read(logID)
		if err != nil {
			t.Fatalf("failed read log[%d] with err=%s", i, err)
		}

		if !logs[i].Equals(actualLog) {
			t.Fatalf("logs not equals: expected=%+v actual=%+v", logs[i], actualLog)
		}
	}
}

func openTestFile(t *testing.T, name string) *os.File {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), name)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("failed open test file(%s) with err=%s", name, err)
	}

	t.Cleanup(func() {
		if closeErr := file.Close(); closeErr != nil {
			t.Fatalf("failed close test file(%s) with err=%s", name, closeErr)
		}
	})

	return file
}

func genKeyValueLogs(t *testing.T, count int) []*binlog.KeyValueLog {
	t.Helper()

	logs := make([]*binlog.KeyValueLog, 0, count)
	for i := range count {
		seed := rand.Int63()

		key := []byte("key-" + strconv.FormatInt(int64(i), 10))
		value := []byte("value-" + base32.StdEncoding.EncodeToString([]byte(strconv.FormatInt(seed, 10))))

		logs = append(logs,
			binlog.NewKeyValueLog(int64(i), key, value),
		)
	}
	return logs
}
