package otlpsource_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/type/httpsource"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

var (
	logsProtoMarshaler   = &plog.ProtoMarshaler{}
	logsJSONMarshaler    = &plog.JSONMarshaler{}
	metricsProtoMarshaler = &pmetric.ProtoMarshaler{}
	tracesProtoMarshaler = &ptrace.ProtoMarshaler{}
)

type otlpTestState struct {
	ctx         context.Context
	url         string
	clk         *clockwork.FakeClock
	d           dependencies.ServiceScope
	mock        dependencies.Mocked
	validSecret string
	branchKey   key.BranchKey
	source      definition.Source
	sinkLogs    definition.Sink
	sinkMetrics definition.Sink
	sinkTraces  definition.Sink
	sinkAll     definition.Sink
}

//nolint:tparallel
func TestOTLPSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ts := &otlpTestState{}
	ts.ctx = ctx
	ts.validSecret = strings.Repeat("1", 48)

	port := netutils.FreePortForTest(t)
	ts.url = fmt.Sprintf("http://localhost:%d", port)
	ts.clk = clockwork.NewFakeClock()
	ts.d, ts.mock = dependencies.NewMockedServiceScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", port)
		cfg.Source.HTTP.MaxRequestBodySize = 1 * datasize.MB
	}, commonDeps.WithClock(ts.clk))

	// Create branch, OTLP source and four sinks with different signal filters.
	ts.branchKey = key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(ts.branchKey)

	ts.source = test.NewOTLPSource(key.SourceKey{BranchKey: ts.branchKey, SourceID: "my-source"})
	ts.source.OTLP.Secret = ts.validSecret

	sinkLogsKey := key.SinkKey{SourceKey: ts.source.SourceKey, SinkID: "sink-logs"}
	sinkMetricsKey := key.SinkKey{SourceKey: ts.source.SourceKey, SinkID: "sink-metrics"}
	sinkTracesKey := key.SinkKey{SourceKey: ts.source.SourceKey, SinkID: "sink-traces"}
	sinkAllKey := key.SinkKey{SourceKey: ts.source.SourceKey, SinkID: "sink-all"}

	ts.sinkLogs = dummy.NewSink(sinkLogsKey)
	ts.sinkLogs.AllowedSignals = []string{definition.OTLPSignalLogs}

	ts.sinkMetrics = dummy.NewSink(sinkMetricsKey)
	ts.sinkMetrics.AllowedSignals = []string{definition.OTLPSignalMetrics}

	ts.sinkTraces = dummy.NewSink(sinkTracesKey)
	ts.sinkTraces.AllowedSignals = []string{definition.OTLPSignalTraces}

	ts.sinkAll = dummy.NewSink(sinkAllKey)
	// AllowedSignals empty = accept everything

	repo := ts.d.DefinitionRepository()
	require.NoError(t, repo.Branch().Create(&branch, ts.clk.Now(), test.ByUser()).Do(ts.ctx).Err())
	require.NoError(t, repo.Source().Create(&ts.source, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, repo.Sink().Create(&ts.sinkLogs, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, repo.Sink().Create(&ts.sinkMetrics, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, repo.Sink().Create(&ts.sinkTraces, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, repo.Sink().Create(&ts.sinkAll, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())

	require.NoError(t, stream.StartComponents(ts.ctx, ts.d, ts.mock.TestConfig(), stream.ComponentHTTPSource))
	require.NoError(t, netutils.WaitForHTTP(ts.url, 10*time.Second))

	runOTLPTestCases(t, ts)

	ts.d.Process().Shutdown(ts.ctx, errors.New("bye bye"))
	ts.d.Process().WaitForShutdown()
}

func runOTLPTestCases(t *testing.T, ts *otlpTestState) {
	t.Helper()

	baseURL := fmt.Sprintf("/otlp/123/my-source/%s", ts.validSecret)
	wrongSecret := strings.Repeat("0", 48)

	for _, tc := range []struct {
		name               string
		prepare            func(t *testing.T)
		method             string
		path               string
		headers            map[string]string
		body               []byte
		expectedStatusCode int
		expectedHeaders    map[string]string
		expectedBodyJSON   string
	}{
		// ---- transport / routing errors -------------------------------------------

		{
			name:               "OPTIONS CORS preflight",
			method:             http.MethodOptions,
			path:               baseURL + "/v1/logs",
			expectedStatusCode: http.StatusOK,
			expectedHeaders: map[string]string{
				"Allow":                         "OPTIONS, POST",
				"Access-Control-Allow-Methods":  "OPTIONS, POST",
				"Access-Control-Allow-Headers":  "*",
				"Access-Control-Allow-Origin":   "*",
				"Server":                        httpsource.ServerHeader,
			},
		},
		{
			name:               "unsupported content-type",
			method:             http.MethodPost,
			path:               baseURL + "/v1/logs",
			headers:            map[string]string{"Content-Type": "text/plain"},
			body:               []byte("hello"),
			expectedStatusCode: http.StatusUnsupportedMediaType,
		},
		{
			name:               "invalid project ID",
			method:             http.MethodPost,
			path:               "/otlp/foo/my-source/" + ts.validSecret + "/v1/logs",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "source not found - wrong secret",
			method:             http.MethodPost,
			path:               "/otlp/123/my-source/" + wrongSecret + "/v1/logs",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name: "source disabled",
			prepare: func(t *testing.T) {
				t.Helper()
				require.NoError(t, ts.d.DefinitionRepository().Source().Disable(
					ts.source.SourceKey, ts.clk.Now(), test.ByUser(), "maintenance",
				).Do(ts.ctx).Err())
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					ts.mock.DebugLogger().AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %s","component":"http-source.dispatcher"}`)
				}, 5*time.Second, 50*time.Millisecond)
			},
			method:             http.MethodPost,
			path:               baseURL + "/v1/logs",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusNotFound,
		},

		// ---- re-enable source for remaining tests ----------------------------------

		{
			name: "re-enable source",
			prepare: func(t *testing.T) {
				t.Helper()
				require.NoError(t, ts.d.DefinitionRepository().Source().Enable(
					ts.source.SourceKey, ts.clk.Now(), test.ByUser(),
				).Do(ts.ctx).Err())
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					ts.mock.DebugLogger().AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %s","component":"http-source.dispatcher"}`)
				}, 5*time.Second, 50*time.Millisecond)
			},
			method:             http.MethodPost,
			path:               baseURL + "/v1/logs",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusOK,
		},

		// ---- empty batches --------------------------------------------------------

		{
			name:               "empty logs batch",
			method:             http.MethodPost,
			path:               baseURL + "/v1/logs",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalLogs(t, plog.NewLogs()),
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "empty metrics batch",
			method:             http.MethodPost,
			path:               baseURL + "/v1/metrics",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalMetrics(t, pmetric.NewMetrics()),
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "empty traces batch",
			method:             http.MethodPost,
			path:               baseURL + "/v1/traces",
			headers:            map[string]string{"Content-Type": "application/x-protobuf"},
			body:               mustMarshalTraces(t, ptrace.NewTraces()),
			expectedStatusCode: http.StatusOK,
		},

		// ---- logs -----------------------------------------------------------------

		{
			name:   "logs - protobuf",
			method: http.MethodPost,
			path:   baseURL + "/v1/logs",
			headers: map[string]string{
				"Content-Type": "application/x-protobuf",
			},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusOK,
			expectedHeaders:    map[string]string{"Content-Type": "application/x-protobuf"},
		},
		{
			name:   "logs - application/protobuf alias",
			method: http.MethodPost,
			path:   baseURL + "/v1/logs",
			headers: map[string]string{
				"Content-Type": "application/protobuf",
			},
			body:               mustMarshalLogs(t, sampleLogs()),
			expectedStatusCode: http.StatusOK,
			expectedHeaders:    map[string]string{"Content-Type": "application/x-protobuf"},
		},
		{
			name:   "logs - JSON",
			method: http.MethodPost,
			path:   baseURL + "/v1/logs",
			headers: map[string]string{
				"Content-Type": "application/json",
			},
			body:               mustMarshalLogsJSON(t, sampleLogs()),
			expectedStatusCode: http.StatusOK,
			expectedHeaders:    map[string]string{"Content-Type": "application/json"},
		},
		{
			name:   "logs - gzip compressed protobuf",
			method: http.MethodPost,
			path:   baseURL + "/v1/logs",
			headers: map[string]string{
				"Content-Type":     "application/x-protobuf",
				"Content-Encoding": "gzip",
			},
			body:               mustGzip(t, mustMarshalLogs(t, sampleLogs())),
			expectedStatusCode: http.StatusOK,
		},

		// ---- metrics --------------------------------------------------------------

		{
			name:   "metrics - protobuf",
			method: http.MethodPost,
			path:   baseURL + "/v1/metrics",
			headers: map[string]string{
				"Content-Type": "application/x-protobuf",
			},
			body:               mustMarshalMetrics(t, sampleMetrics()),
			expectedStatusCode: http.StatusOK,
		},

		// ---- traces ---------------------------------------------------------------

		{
			name:   "traces - protobuf",
			method: http.MethodPost,
			path:   baseURL + "/v1/traces",
			headers: map[string]string{
				"Content-Type": "application/x-protobuf",
			},
			body:               mustMarshalTraces(t, sampleTraces()),
			expectedStatusCode: http.StatusOK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.prepare != nil {
				tc.prepare(t)
			}
			ts.mock.DebugLogger().Truncate()

			req, err := http.NewRequestWithContext(ts.ctx, tc.method, ts.url+tc.path, bytes.NewReader(tc.body))
			require.NoError(t, err)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()

			assert.Equal(t, tc.expectedStatusCode, resp.StatusCode, "status code")

			for k, v := range tc.expectedHeaders {
				assert.Equal(t, v, resp.Header.Get(k), "header %s", k)
			}

			if tc.expectedBodyJSON != "" {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.JSONEq(t, tc.expectedBodyJSON, string(body))
			}
		})
	}
}

func TestOTLPSource_SignalRouting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	port := netutils.FreePortForTest(t)
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", port)
	}, commonDeps.WithClock(clk))

	validSecret := strings.Repeat("2", 48)
	branchKey := key.BranchKey{ProjectID: 456, BranchID: 1}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "routing-source"}

	branch := test.NewBranch(branchKey)
	source := test.NewOTLPSource(sourceKey)
	source.OTLP.Secret = validSecret

	sinkLogs := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "logs-sink"})
	sinkLogs.AllowedSignals = []string{definition.OTLPSignalLogs}

	sinkMetrics := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "metrics-sink"})
	sinkMetrics.AllowedSignals = []string{definition.OTLPSignalMetrics}

	sinkTraces := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "traces-sink"})
	sinkTraces.AllowedSignals = []string{definition.OTLPSignalTraces}

	repo := d.DefinitionRepository()
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, repo.Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, repo.Sink().Create(&sinkLogs, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, repo.Sink().Create(&sinkMetrics, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, repo.Sink().Create(&sinkTraces, clk.Now(), test.ByUser(), "create").Do(ctx).Err())

	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentHTTPSource))

	baseURL := fmt.Sprintf("http://localhost:%d/otlp/456/routing-source/%s", port, validSecret)
	require.NoError(t, netutils.WaitForHTTP(fmt.Sprintf("http://localhost:%d", port), 10*time.Second))

	// Each signal endpoint must return 200; AllowedSignals filtering is exercised via the router.
	resp := doOTLPPost(t, ctx, baseURL+"/v1/logs", "application/x-protobuf", mustMarshalLogs(t, sampleLogs()))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	resp = doOTLPPost(t, ctx, baseURL+"/v1/metrics", "application/x-protobuf", mustMarshalMetrics(t, sampleMetrics()))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	resp = doOTLPPost(t, ctx, baseURL+"/v1/traces", "application/x-protobuf", mustMarshalTraces(t, sampleTraces()))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_ = resp.Body.Close()

	d.Process().Shutdown(ctx, errors.New("done"))
	d.Process().WaitForShutdown()
}

func TestOTLPSource_PartialSuccess(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	port := netutils.FreePortForTest(t)
	clk := clockwork.NewFakeClock()
	d, mock := dependencies.NewMockedServiceScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", port)
	}, commonDeps.WithClock(clk))

	validSecret := strings.Repeat("3", 48)
	branchKey := key.BranchKey{ProjectID: 789, BranchID: 1}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "partial-source"}

	branch := test.NewBranch(branchKey)
	source := test.NewOTLPSource(sourceKey)
	source.OTLP.Secret = validSecret

	ctrl := mock.TestDummySinkController()
	ctrl.PipelineWriteError = errors.New("disk full")

	sink := dummy.NewSink(key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"})

	repo := d.DefinitionRepository()
	require.NoError(t, repo.Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, repo.Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, repo.Sink().Create(&sink, clk.Now(), test.ByUser(), "create").Do(ctx).Err())

	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentHTTPSource))
	require.NoError(t, netutils.WaitForHTTP(fmt.Sprintf("http://localhost:%d", port), 10*time.Second))

	// A logs batch with 2 records where the sink fails — expect 200 with partial_success body.
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	sl := rl.ScopeLogs().AppendEmpty()
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.Body().SetStr("record one")
	lr2 := sl.LogRecords().AppendEmpty()
	lr2.Body().SetStr("record two")

	baseURL := fmt.Sprintf("http://localhost:%d/otlp/789/partial-source/%s", port, validSecret)
	resp := doOTLPPost(t, ctx, baseURL+"/v1/logs", "application/x-protobuf", mustMarshalLogs(t, logs))

	// When all records fail with a retryable error, the handler escalates to a top-level 5xx.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	d.Process().Shutdown(ctx, errors.New("done"))
	d.Process().WaitForShutdown()
}

// ---- sample OTel payload helpers --------------------------------------------

func sampleLogs() plog.Logs {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "auth-service")
	rl.Resource().Attributes().PutStr("deployment.environment", "production")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/my/auth")
	sl.Scope().SetVersion("1.2.3")

	lr := sl.LogRecords().AppendEmpty()
	ts := pcommon.NewTimestampFromTime(time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC))
	lr.SetTimestamp(ts)
	lr.SetSeverityNumber(plog.SeverityNumberInfo)
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("User login successful")
	lr.Attributes().PutStr("user.id", "u-42")
	lr.Attributes().PutStr("request.id", "req-abc123")
	return logs
}

func sampleMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "cart-service")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/my/cart")

	m := sm.Metrics().AppendEmpty()
	m.SetName("http.server.request.duration")
	m.SetDescription("Duration of HTTP server requests.")
	m.SetUnit("s")

	hist := m.SetEmptyHistogram()
	hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	dp := hist.DataPoints().AppendEmpty()
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 6, 1, 12, 1, 0, 0, time.UTC)))
	dp.SetCount(100)
	dp.SetSum(4.5)
	dp.Attributes().PutStr("http.method", "GET")
	dp.Attributes().PutStr("http.status_code", "200")
	return metrics
}

func sampleTraces() ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "order-service")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("github.com/my/orders")

	span := ss.Spans().AppendEmpty()
	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
	span.SetName("POST /orders")
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 6, 1, 12, 0, 0, 50_000_000, time.UTC)))
	span.Attributes().PutStr("http.method", "POST")
	span.Attributes().PutInt("http.status_code", 201)

	event := span.Events().AppendEmpty()
	event.SetName("order.created")
	event.Attributes().PutStr("order.id", "ord-9999")
	return traces
}

// ---- marshal helpers --------------------------------------------------------

func mustMarshalLogs(t *testing.T, logs plog.Logs) []byte {
	t.Helper()
	b, err := logsProtoMarshaler.MarshalLogs(logs)
	require.NoError(t, err)
	return b
}

func mustMarshalLogsJSON(t *testing.T, logs plog.Logs) []byte {
	t.Helper()
	b, err := logsJSONMarshaler.MarshalLogs(logs)
	require.NoError(t, err)
	return b
}

func mustMarshalMetrics(t *testing.T, metrics pmetric.Metrics) []byte {
	t.Helper()
	b, err := metricsProtoMarshaler.MarshalMetrics(metrics)
	require.NoError(t, err)
	return b
}

func mustMarshalTraces(t *testing.T, traces ptrace.Traces) []byte {
	t.Helper()
	b, err := tracesProtoMarshaler.MarshalTraces(traces)
	require.NoError(t, err)
	return b
}

func mustGzip(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func doOTLPPost(t *testing.T, ctx context.Context, url, contentType string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}
