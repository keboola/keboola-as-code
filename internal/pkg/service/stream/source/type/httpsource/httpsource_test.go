package httpsource_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/type/httpsource"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type TestCase struct {
	Name               string
	Prepare            func(t *testing.T)
	Method             string
	Path               string
	Query              string
	Headers            map[string]string
	Body               io.Reader
	ExpectedErr        string
	ExpectedStatusCode int
	ExpectedHeaders    map[string]string
	ExpectedBody       string
	ExpectedLogs       string
}

type testState struct {
	ctx             context.Context
	logger          log.DebugLogger
	url             string
	maxHeaderSize   int
	maxBodySize     int
	clk             *clockwork.FakeClock
	d               dependencies.ServiceScope
	mock            dependencies.Mocked
	validSecret     string
	invalidSecret   string
	branchAKey      key.BranchKey
	branchBKey      key.BranchKey
	branchA         definition.Branch
	branchB         definition.Branch
	source1A        definition.Source
	source1B        definition.Source
	source2Disabled definition.Source
	sink1A1         definition.Sink
	sink1B1         definition.Sink
	sink1A2Disabled definition.Sink
	sink1B2Disabled definition.Sink
}

//nolint:tparallel // we want to run the subtests - requests sequentially and check the logs
func TestHTTPSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	ts := &testState{}
	ts.ctx = ctx
	ts.maxHeaderSize = 2000
	ts.maxBodySize = 8000

	// Dependencies
	port := netutils.FreePortForTest(t)
	listenAddr := fmt.Sprintf("localhost:%d", port)
	ts.url = fmt.Sprintf(`http://%s`, listenAddr)
	ts.clk = clockwork.NewFakeClock()
	ts.d, ts.mock = dependencies.NewMockedServiceScopeWithConfig(t, ctx, func(cfg *config.Config) {
		cfg.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", port)
		cfg.Source.HTTP.ReadBufferSize = datasize.ByteSize(ts.maxHeaderSize) * datasize.B // ReadBufferSize is a limit for headers, not for the body
		cfg.Source.HTTP.MaxRequestBodySize = datasize.ByteSize(ts.maxBodySize) * datasize.B
	}, commonDeps.WithClock(ts.clk))
	ts.logger = ts.mock.DebugLogger()

	// Create sources and sinks
	ts.validSecret = strings.Repeat("1", 48)
	ts.invalidSecret = strings.Repeat("0", 48)
	ts.branchAKey = key.BranchKey{ProjectID: 123, BranchID: 111}
	ts.branchBKey = key.BranchKey{ProjectID: 123, BranchID: 222}
	ts.branchA = test.NewBranch(ts.branchAKey)
	ts.branchB = test.NewBranch(ts.branchBKey)
	ts.source1A = test.NewHTTPSource(key.SourceKey{BranchKey: ts.branchAKey, SourceID: "my-source-1"})
	ts.source1A.HTTP.Secret = ts.validSecret
	ts.source1B = test.NewHTTPSource(key.SourceKey{BranchKey: ts.branchBKey, SourceID: "my-source-1"})
	ts.source1B.HTTP.Secret = ts.validSecret
	ts.source2Disabled = test.NewHTTPSource(key.SourceKey{BranchKey: ts.branchAKey, SourceID: "my-source-2"})
	ts.source2Disabled.HTTP.Secret = ts.validSecret
	ts.sink1A1 = dummy.NewSink(key.SinkKey{SourceKey: ts.source1A.SourceKey, SinkID: "my-sink-1"})
	ts.sink1B1 = dummy.NewSink(key.SinkKey{SourceKey: ts.source1B.SourceKey, SinkID: "my-sink-1"})
	ts.sink1A2Disabled = dummy.NewSink(key.SinkKey{SourceKey: ts.source1A.SourceKey, SinkID: "my-sink-2"})
	ts.sink1B2Disabled = dummy.NewSink(key.SinkKey{SourceKey: ts.source1B.SourceKey, SinkID: "my-sink-2"})
	require.NoError(t, ts.d.DefinitionRepository().Branch().Create(&ts.branchA, ts.clk.Now(), test.ByUser()).Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Branch().Create(&ts.branchB, ts.clk.Now(), test.ByUser()).Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Source().Create(&ts.source1A, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Source().Create(&ts.source1B, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Source().Create(&ts.source2Disabled, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Source().Disable(ts.source2Disabled.SourceKey, ts.clk.Now(), test.ByUser(), "reason").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Create(&ts.sink1A1, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Create(&ts.sink1B1, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Create(&ts.sink1A2Disabled, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Create(&ts.sink1B2Disabled, ts.clk.Now(), test.ByUser(), "create").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Disable(ts.sink1A2Disabled.SinkKey, ts.clk.Now(), test.ByUser(), "reason").Do(ts.ctx).Err())
	require.NoError(t, ts.d.DefinitionRepository().Sink().Disable(ts.sink1B2Disabled.SinkKey, ts.clk.Now(), test.ByUser(), "reason").Do(ts.ctx).Err())

	// Start
	require.NoError(t, stream.StartComponents(ts.ctx, ts.d, ts.mock.TestConfig(), stream.ComponentHTTPSource))

	// Wait for the HTTP server
	require.NoError(t, netutils.WaitForHTTP(ts.url, 10*time.Second))
	ts.logger.AssertJSONMessages(t, `
{"level":"info","message":"starting HTTP source node","component":"http-source"}
{"level":"info","message":"started HTTP source on \"0.0.0.0:%d\"","component":"http-source"}
`)

	// Send testing requests
	sendTestRequests(t, ts)

	// Shutdown
	ts.logger.Truncate()
	ts.d.Process().Shutdown(ts.ctx, errors.New("bye bye"))
	ts.d.Process().WaitForShutdown()
	ts.logger.AssertJSONMessages(t, `
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"shutting down HTTP source at \"0.0.0.0:%d\"","component":"http-source"}
{"level":"info","message":"HTTP source shutdown done","component":"http-source"}
{"level":"info","message":"closing sink router","component":"sink.router"}
{"level":"info","message":"watch stream consumer closed: context canceled","component":"sink.router"}
{"level":"info","message":"closed sink router","component":"sink.router"}
{"level":"info","message":"closing volumes stream","component":"volume.repository"}
{"level":"info","message":"closed volumes stream","component":"volume.repository"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}

func testCases(t *testing.T, ts *testState) []TestCase {
	t.Helper()

	require.Less(t, ts.maxHeaderSize, ts.maxBodySize)

	return []TestCase{
		{
			Name:               "health check",
			Method:             http.MethodGet,
			Path:               "/health-check",
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody:       "OK\n",
		},
		{
			Name:               "not found",
			Method:             http.MethodGet,
			Path:               "/foo",
			ExpectedStatusCode: http.StatusNotFound,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedLogs:       `{"level":"info","message":"not found, please send data using POST /stream/<projectID>/<sourceID>/<secret>"}`,
			ExpectedBody: `
{
  "statusCode": 404,
  "error": "stream.in.routeNotFound",
  "message": "Not found, please send data using POST /stream/\u003cprojectID\u003e/\u003csourceID\u003e/\u003csecret\u003e"
}`,
		},
		{
			Name:               "stream input - OPTIONS",
			Method:             http.MethodOptions,
			Path:               "/stream/1234/my-source/my-secret",
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders: map[string]string{
				"Allow":                         "OPTIONS, POST",
				"Access-Control-Allow-Methods":  "OPTIONS, POST",
				"Access-Control-Allow-Headers":  "*",
				"Access-Control-Expose-Headers": "*",
				"Access-Control-Allow-Origin":   "*",
				"Content-Length":                "0",
				"Server":                        httpsource.ServerHeader,
			},
		},
		{
			Name:               "stream input - POST - invalid project ID",
			Method:             http.MethodPost,
			Path:               "/stream/foo/my-source/my-secret",
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusBadRequest,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 400,
  "error": "stream.in.badRequest",
  "message": "Invalid project ID \"foo\"."
}`,
		},
		{
			Name:               "stream input - POST - not found",
			Method:             http.MethodPost,
			Path:               "/stream/1111/my-source/my-secret",
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusNotFound,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 404,
  "error": "stream.in.noSourceFound",
  "message": "The specified combination of projectID, sourceID and secret was not found."
}`,
			ExpectedLogs: `{"level":"warn","message":"dispatch failed","nodeId":"test-node","project.id":"1111","source.id":"my-source","component":"http-source"}`,
		},
		{
			Name:               "stream input - POST - not found - invalid secret",
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.invalidSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusNotFound,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 404,
  "error": "stream.in.noSourceFound",
  "message": "The specified combination of projectID, sourceID and secret was not found."
}`,
			ExpectedLogs: `{"level":"warn","message":"dispatch failed","nodeId":"test-node","project.id":"123","source.id":"my-source-1","component":"http-source"}`,
		},
		{
			Name:               "stream input - POST - not found - disabled source",
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-2/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusNotFound,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 404,
  "error": "stream.in.disabledSource",
  "message": "The specified source is disabled in all branches."
}`,
			ExpectedLogs: `{"level":"warn","message":"dispatch failed","nodeId":"test-node","project.id":"123","source.id":"my-source-2","component":"http-source"}`,
		},
		{
			Name: "stream input - POST - open pipeline error",

			Prepare: func(t *testing.T) {
				t.Helper()
				c := ts.mock.TestDummySinkController()
				c.PipelineOpenError = errors.New("some open error")
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusInternalServerError,
			ExpectedHeaders: map[string]string{
				"Content-Type": "application/json",
				"Server":       httpsource.ServerHeader,
			},
			ExpectedLogs: `
{"level":"error","message":"write record error: cannot open sink pipeline: some open error, next attempt after %s","component":"sink.router"}
{"level":"error","message":"write record error: cannot open sink pipeline: some open error, next attempt after %s","component":"sink.router"}
`,
			ExpectedBody: `
{
  "statusCode": 500,
  "error": "stream.in.writeFailed",
  "message": "Written to 0/2 sinks.",
  "sources": [
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 111,
      "statusCode": 500,
      "error": "stream.in.writeFailed",
      "message": "Written to 0/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 500,
          "error": "stream.in.genericError",
          "message": "Cannot open sink pipeline: some open error, next attempt after %s."
        }
      ]
    },
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 222,
      "statusCode": 500,
      "error": "stream.in.writeFailed",
      "message": "Written to 0/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 500,
          "error": "stream.in.genericError",
          "message": "Cannot open sink pipeline: some open error, next attempt after %s."
        }
      ]
    }
  ]
}`,
		},
		{
			Name: "stream input - POST - write error",
			Prepare: func(t *testing.T) {
				t.Helper()
				ts.clk.Advance(10 * time.Second) // skip backoff delay for open pipeline operation
				c := ts.mock.TestDummySinkController()
				c.PipelineOpenError = nil
				c.PipelineWriteError = errors.New("some write error")
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusInternalServerError,
			ExpectedHeaders: map[string]string{
				"Content-Type": "application/json",
				"Server":       httpsource.ServerHeader,
			},
			ExpectedLogs: `
{"level":"error","message":"write record error: some write error","component":"sink.router"}
{"level":"error","message":"write record error: some write error","component":"sink.router"}
`,
			ExpectedBody: `
{
  "statusCode": 500,
  "error": "stream.in.writeFailed",
  "message": "Written to 0/2 sinks.",
  "sources": [
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 111,
      "statusCode": 500,
      "error": "stream.in.writeFailed",
      "message": "Written to 0/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 500,
          "error": "stream.in.genericError",
          "message": "Some write error."
        }
      ]
    },
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 222,
      "statusCode": 500,
      "error": "stream.in.writeFailed",
      "message": "Written to 0/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 500,
          "error": "stream.in.genericError",
          "message": "Some write error."
        }
      ]
    }
  ]
}`,
		},
		{
			Name: "stream input - POST - ok - accepted",
			Prepare: func(t *testing.T) {
				t.Helper()
				c := ts.mock.TestDummySinkController()
				c.PipelineWriteError = nil
				c.PipelineWriteRecordStatus = pipeline.RecordAccepted
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusAccepted,
			ExpectedHeaders: map[string]string{
				"Content-Type": "text/plain",
				"Server":       httpsource.ServerHeader,
			},
			ExpectedBody: "OK",
		},
		{
			Name: "stream input - POST - ok - processed",
			Prepare: func(t *testing.T) {
				t.Helper()
				c := ts.mock.TestDummySinkController()
				c.PipelineWriteError = nil
				c.PipelineWriteRecordStatus = pipeline.RecordProcessed
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders: map[string]string{
				"Content-Type": "text/plain",
				"Server":       httpsource.ServerHeader,
			},
			ExpectedBody: "OK",
		},
		{
			Name: "stream input - POST - ok - accepted - verbose",
			Prepare: func(t *testing.T) {
				t.Helper()
				c := ts.mock.TestDummySinkController()
				c.PipelineWriteError = nil
				c.PipelineWriteRecordStatus = pipeline.RecordAccepted
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Query:              "verbose=true",
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusAccepted,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 202,
  "message": "Successfully written to 2/2 sinks.",
  "sources": [
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 111,
      "statusCode": 202,
      "message": "Successfully written to 1/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 202,
          "message": "accepted"
        }
      ]
    },
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 222,
      "statusCode": 202,
      "message": "Successfully written to 1/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 202,
          "message": "accepted"
        }
      ]
    }
  ]
}`,
		},
		{
			Name: "stream input - POST - ok - processed - verbose",
			Prepare: func(t *testing.T) {
				t.Helper()
				c := ts.mock.TestDummySinkController()
				c.PipelineWriteError = nil
				c.PipelineWriteRecordStatus = pipeline.RecordProcessed
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Query:              "verbose=true",
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize)),
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 200,
  "message": "Successfully written to 2/2 sinks.",
  "sources": [
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 111,
      "statusCode": 200,
      "message": "Successfully written to 1/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 200,
          "message": "processed"
        }
      ]
    },
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 222,
      "statusCode": 200,
      "message": "Successfully written to 1/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 200,
          "message": "processed"
        }
      ]
    }
  ]
}`,
		},
		{
			Name:               "stream input - POST - over maximum header size",
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Headers:            map[string]string{"foo": strings.Repeat(".", ts.maxHeaderSize+1)},
			ExpectedStatusCode: http.StatusRequestEntityTooLarge,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedLogs:       `{"level":"info","message":"request header size is over the maximum \"2000B\"","error.type":"%s/errors.HeaderTooLargeError"}`,
			ExpectedBody: `
{
  "statusCode": 413,
  "error": "stream.in.headerTooLarge",
  "message": "Request header size is over the maximum \"2000B\"."
}`,
		},
		{
			Name:               "stream input - POST - over maximum body size",
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source/" + ts.validSecret,
			Body:               strings.NewReader(strings.Repeat(".", ts.maxBodySize+1)),
			ExpectedStatusCode: http.StatusRequestEntityTooLarge,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedLogs:       `{"level":"info","message":"request body size is over the maximum \"8000B\"","error.type":"%s/errors.BodyTooLargeError"}`,
			ExpectedBody: `
{
  "statusCode": 413,
  "error": "stream.in.bodyTooLarge",
  "message": "Request body size is over the maximum \"8000B\"."
}`,
		},
		{
			Name: "stream input - POST - disable sink",
			Prepare: func(t *testing.T) {
				t.Helper()

				// Disable the sink
				require.NoError(t, ts.d.DefinitionRepository().Sink().Disable(ts.sink1B1.SinkKey, ts.clk.Now(), test.ByUser(), "reason").Do(ts.ctx).Err())

				// Wait for the router sync
				assert.EventuallyWithT(t, func(c *assert.CollectT) {
					ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed sink pipeline:%s","branch.id":"222","project.id":"123","sink.id":"my-sink-1","source.id":"my-source-1","component":"sink.router"}
`)
				}, 10*time.Second, 100*time.Millisecond)
			},
			Method:             http.MethodPost,
			Path:               "/stream/123/my-source-1/" + ts.validSecret,
			Query:              "verbose=true",
			Body:               strings.NewReader("foo"),
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders:    map[string]string{"Server": httpsource.ServerHeader},
			ExpectedBody: `
{
  "statusCode": 200,
  "message": "Successfully written to 1/1 sinks.",
  "sources": [
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 111,
      "statusCode": 200,
      "message": "Successfully written to 1/1 sinks.",
      "sinks": [
        {
          "sinkId": "my-sink-1",
          "statusCode": 200,
          "message": "processed"
        }
      ]
    },
    {
      "projectId": 123,
      "sourceId": "my-source-1",
      "branchId": 222,
      "statusCode": 200,
      "message": "No enabled sink found."
    }
  ]
}`,
		},
	}
}

func sendTestRequests(t *testing.T, f *testState) {
	t.Helper()

	logger := f.mock.DebugLogger()

	for _, tc := range testCases(t, f) {
		t.Run(strhelper.NormalizeName(tc.Name), func(t *testing.T) {
			if tc.Prepare != nil {
				tc.Prepare(t)
			}

			logger.Truncate()

			// URL
			url := f.url + tc.Path
			if tc.Query != "" {
				url += "?" + tc.Query
			}

			// Method, URL, body
			require.NotEmpty(t, tc.Method)
			req, err := http.NewRequestWithContext(f.ctx, tc.Method, url, tc.Body)
			require.NoError(t, err)

			// Headers
			for k, v := range tc.Headers {
				req.Header.Set(k, v)
			}

			// Error + logs
			resp, err := http.DefaultClient.Do(req)
			assert.EventuallyWithT(t, func(c *assert.CollectT) {
				logger.AssertJSONMessages(c, tc.ExpectedLogs)
			}, 5*time.Second, 10*time.Millisecond)
			if tc.ExpectedErr != "" {
				if assert.Error(t, err) {
					wildcards.Assert(t, tc.ExpectedErr, err.Error())
				}
				return
			}

			// Expected status code
			require.NoError(t, err)
			assert.Equal(t, tc.ExpectedStatusCode, resp.StatusCode)

			// Expected headers
			if len(tc.ExpectedHeaders) > 0 {
				for k, v := range tc.ExpectedHeaders {
					assert.Equal(t, v, resp.Header.Get(k), `key=%s`, k)
				}
			}

			// Response body
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			wildcards.Assert(t, tc.ExpectedBody, string(respBody))
		})
	}
}
