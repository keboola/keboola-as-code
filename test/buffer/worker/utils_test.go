package worker

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/keboola/go-utils/pkg/wildcards"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
	"golang.org/x/sync/errgroup"

	apiModel "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	apiServer "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/http/buffer/server"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

//nolint:forbidigo
func (ts *testSuite) AssertEtcdState(expectedFile string, opts ...etcdhelper.AssertOption) {
	dump, err := etcdhelper.DumpAllToString(ts.ctx, ts.etcdClient)
	require.NoError(ts.t, err)

	// Write actual state
	require.NoError(ts.t, os.WriteFile(filepath.Join(ts.etcdOutDir, fmt.Sprintf(`actual-%s.txt`, expectedFile)), []byte(dump), 0o644))

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
	etcdhelper.AssertKVs(ts.t, ts.etcdClient, expectedKVs, opts...)
}

// IterateMetrics scrapes /metrics endpoint for each API node. Parsed metrics are streamed to the channel.
func (ts *testSuite) IterateMetrics(fn func(<-chan *dto.MetricFamily)) {
	ch := make(chan *dto.MetricFamily)

	// Scrape /metrics endpoint of all API nodes
	grp, _ := errgroup.WithContext(ts.ctx)
	for _, n := range ts.apiNodes {
		n := n
		grp.Go(func() error {
			// Get metrics from the endpoint
			var resp []byte
			req := request.NewHTTPRequest(n.MetricsClient).WithGet("/metrics").WithResult(&resp)
			if err := req.SendOrErr(ts.ctx); err != nil {
				return err
			}

			// Parse metrisc
			var parser expfmt.TextParser
			metricFamilies, err := parser.TextToMetricFamilies(bytes.NewReader(resp))
			if err != nil {
				return err
			}

			for _, f := range metricFamilies {
				ch <- f
			}

			return nil
		})
	}

	go fn(ch)

	assert.NoError(ts.t, grp.Wait())
	close(ch)
}

// WaitForLogMessages wait until the lines are logged or a timeout occurs.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) WaitForLogMessages(timeout time.Duration, lines string) bool {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	return assert.Eventually(ts.t, func() bool {
		return wildcards.Compare(expected, ts.logsOut.String()) == nil
	}, timeout, 100*time.Millisecond, ts.logsOut.String())
}

func (ts *testSuite) AssertNoLoggedWarning() {
	msgs := ts.logsOut.String()
	assert.Falsef(ts.t, strings.Contains(msgs, "WARN"), "Found some warning messages: %v", msgs)
}

func (ts *testSuite) AssertNoLoggedError() {
	msgs := ts.logsOut.String()
	assert.Falsef(ts.t, strings.Contains(msgs, "ERROR"), "Found some error messages: %v", msgs)
}

// AssertLoggedLines checks that each requested line has been logged.
// Wildcards can be used.
// The lines do not have to be logged consecutively,
// there can be another line between them, but the order must be preserved.
func (ts *testSuite) AssertLoggedLines(lines string) {
	expected := `%A` + strings.ReplaceAll(strings.TrimSpace(lines), "\n", "\n%A") + `%A`
	wildcards.Assert(ts.t, expected, ts.logsOut.String())
}

// TruncateLogs clear all logs.
func (ts *testSuite) TruncateLogs() {
	// write to stdout if TEST_VERBOSE=true
	_, _ = ts.logsOut.WriteString("------------------------------ TRUNCATE LOGS ------------------------------\n")
	ts.logsOut.Truncate()
}

func columnsModeToBody(columns []*apiModel.Column) (out []*apiServer.ColumnRequestBody) {
	// Map columns type
	for _, c := range columns {
		bodyColumn := &apiServer.ColumnRequestBody{
			PrimaryKey: &c.PrimaryKey,
			Type:       &c.Type,
			Name:       &c.Name,
		}
		if c.Template != nil {
			bodyColumn.Template = &apiServer.TemplateRequestBody{
				Language: &c.Template.Language,
				Content:  &c.Template.Content,
			}
		}
		out = append(out, bodyColumn)
	}
	return out
}
