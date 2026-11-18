package sessionstore

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestRedisLogger_FailedIsWarn(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	newRedisLogger(logger).Printf(t.Context(), "sentinel: GetMasterAddrByName addr=%s, master=%q failed: %s", "10.0.0.1:26379", "mymaster", "context deadline exceeded")
	logger.AssertJSONMessages(t, `{"level":"warn","message":"sentinel: GetMasterAddrByName addr=10.0.0.1:26379, master=\"mymaster\" failed: context deadline exceeded"}`)
}

func TestRedisLogger_OtherIsInfo(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	newRedisLogger(logger).Printf(t.Context(), "sentinel: new master=%q addr=%q", "mymaster", "10.0.0.2:6379")
	logger.AssertJSONMessages(t, `{"level":"info","message":"sentinel: new master=\"mymaster\" addr=\"10.0.0.2:6379\""}`)
}
