package worker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

//nolint:forbidigo
func (ts *testSuite) AssertEtcdState(expectedFile string) {
	dump, err := etcdhelper.DumpAllToString(ts.ctx, ts.etcdClient)
	require.NoError(ts.t, err)

	// Write actual state
	require.NoError(ts.t, os.WriteFile(filepath.Join(ts.outDir, fmt.Sprintf(`actual-%s.txt`, expectedFile)), []byte(dump), 0o644))

	// Load expected state
	content, err := os.ReadFile(filepath.Join(ts.testDir, "expected-etcd-state", fmt.Sprintf("%s.txt", expectedFile)))
	require.NoError(ts.t, err)
	expected := string(content)

	// Process includes
	for {
		found := false
		expected = regexpcache.MustCompile(`(?mU)^<include [^<>\n]+>$`).ReplaceAllStringFunc(expected, func(s string) string {
			found = true
			s = strings.TrimPrefix(s, "<include ")
			s = strings.TrimSuffix(s, ">")
			s = strings.TrimSpace(s)
			path := fmt.Sprintf("%s.txt", s)
			subContent, err := os.ReadFile(filepath.Join(ts.testDir, "expected-etcd-state", path))
			if err != nil {
				assert.Fail(ts.t, fmt.Sprintf(`cannot load included file "%s"`, path))
			}
			return "\n" + string(subContent) + "\n"
		})
		if !found {
			break
		}
	}

	var expectedKVs []etcdhelper.KV
	for _, kv := range etcdhelper.ParseDump(expected) {
		kv.Key, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Key, ts.envs, "%%")
		assert.NoError(ts.t, err)
		kv.Value, err = testhelper.ReplaceEnvsStringWithSeparator(kv.Value, ts.envs, "%%")
		assert.NoError(ts.t, err)
		expectedKVs = append(expectedKVs, kv)
	}

	// Compare
	etcdhelper.AssertKVs(ts.t, ts.etcdClient, expectedKVs, etcdhelper.WithIgnoredKeyPattern(`^stats/`))
}

// WaitForLogMessages wait until the lines are logged or a timeout occurs.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) WaitForLogMessages(timeout time.Duration, lines string) {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	assert.Eventually(ts.t, func() bool {
		return wildcards.Compare(expected, ts.logger.AllMessages()) == nil
	}, timeout, 100*time.Millisecond, ts.logger.AllMessages())
}

func (ts *testSuite) AssertNoLoggedWarning() {
	msgs := ts.logger.WarnMessages()
	assert.Len(ts.t, msgs, 0, "Found some warning messages: %v", msgs)
}

func (ts *testSuite) AssertNoLoggedError() {
	msgs := ts.logger.WarnMessages()
	assert.Len(ts.t, msgs, 0, "Found some error messages: %v", msgs)
}

// AssertLoggedLines checks that each requested line has been logged.
// Wildcards can be used.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) AssertLoggedLines(lines string) {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	wildcards.Assert(ts.t, expected, ts.logger.AllMessages())
}

// TruncateLogs clear all logs.
func (ts *testSuite) TruncateLogs() {
	// write to stdout if TEST_VERBOSE=true
	ts.logger.Info("------------------------------ TRUNCATE LOGS ------------------------------")
	ts.logger.Truncate()
}
