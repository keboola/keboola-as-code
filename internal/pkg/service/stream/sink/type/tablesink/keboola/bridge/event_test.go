package bridge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestBridge_SendSliceUploadEvent_OkEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	var body string
	transport := mock.MockedHTTPTransport()
	registerOkResponder(t, transport, &body)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = error(nil)
	slice := test.NewSlice()
	b.SendSliceUploadEvent(ctx, api, duration, &err, slice.SliceKey, testStats(slice.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"debug","message":"Sent eventID: 12345"}`)
	wildcards.Assert(t, `
{
  "component": "keboola.stream.sliceUpload",
  "duration": 3,
  "message": "Slice upload done.",
  "params": "{\"branchId\":456,\"projectId\":123,\"sinkId\":\"my-sink\",\"sourceId\":\"my-source\",\"streamId\":\"123/456/my-source\"}",
  "results": "{\"statistics\":{\"compressedSize\":52428800,\"firstRecordAt\":\"2000-01-01T20:00:00.000Z\",\"lastRecordAt\":\"2000-01-02T01:00:00.000Z\",\"recordsCount\":123,\"slicesCount\":1,\"stagingSize\":26214400,\"uncompressedSize\":104857600}}",
  "type": "info"
}`, body)
}

func TestBridge_SendSliceUploadEvent_ErrorEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	var body string
	transport := mock.MockedHTTPTransport()
	registerOkResponder(t, transport, &body)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = errors.New("some error")
	slice := test.NewSlice()
	b.SendSliceUploadEvent(ctx, api, duration, &err, slice.SliceKey, testStats(slice.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
	wildcards.Assert(t, `
{
  "component": "keboola.stream.sliceUpload",
  "duration": 3,
  "message": "Slice upload failed.",
  "params": "{\"branchId\":456,\"projectId\":123,\"sinkId\":\"my-sink\",\"sourceId\":\"my-source\",\"streamId\":\"123/456/my-source\"}",
  "results": "{\"error\":\"some error\"}",
  "type": "error"
}`, body)
}

func TestBridge_SendSliceUploadEvent_HTTPError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	transport := mock.MockedHTTPTransport()
	registerErrorResponder(t, transport)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = error(nil)
	slice := test.NewSlice()
	b.SendSliceUploadEvent(ctx, api, duration, &err, slice.SliceKey, testStats(slice.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
}

func TestBridge_SendFileImportEvent_OkEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	var body string
	transport := mock.MockedHTTPTransport()
	registerOkResponder(t, transport, &body)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = error(nil)
	file := test.NewFile()
	b.SendFileImportEvent(ctx, api, duration, &err, file.FileKey, testStats(file.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
	mock.DebugLogger().AssertJSONMessages(t, `{"level":"debug","message":"Sent eventID: 12345"}`)
	wildcards.Assert(t, `
{
  "component": "keboola.stream.fileImport",
  "duration": 3,
  "message": "File import done.",
  "params": "{\"branchId\":456,\"projectId\":123,\"sinkId\":\"my-sink\",\"sourceId\":\"my-source\",\"streamId\":\"123/456/my-source\"}",
  "results": "{\"statistics\":{\"compressedSize\":52428800,\"firstRecordAt\":\"2000-01-01T01:00:00.000Z\",\"lastRecordAt\":\"2000-01-02T01:00:00.000Z\",\"recordsCount\":123,\"slicesCount\":1,\"stagingSize\":26214400,\"uncompressedSize\":104857600}}",
  "type": "info"
}`, body)
}

func TestBridge_SendFileImportEvent_ErrorEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	var body string
	transport := mock.MockedHTTPTransport()
	registerOkResponder(t, transport, &body)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = errors.New("some error")
	file := test.NewFile()
	b.SendFileImportEvent(ctx, api, duration, &err, file.FileKey, testStats(file.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
	wildcards.Assert(t, `
{
  "component": "keboola.stream.fileImport",
  "duration": 3,
  "message": "File import failed.",
  "params": "{\"branchId\":456,\"projectId\":123,\"sinkId\":\"my-sink\",\"sourceId\":\"my-source\",\"streamId\":\"123/456/my-source\"}",
  "results": "{\"error\":\"some error\"}",
  "type": "error"
}`, body)
}

func TestBridge_SendFileImportEvent_HTTPError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	api := d.KeboolaPublicAPI().NewAuthorizedAPI("my-token", 1*time.Minute)

	transport := mock.MockedHTTPTransport()
	registerErrorResponder(t, transport)

	cfg := keboolasink.NewConfig()
	// Send event
	b, err := bridge.New(d, nil, cfg)
	require.NoError(t, err)
	now := utctime.MustParse("2000-01-02T01:00:00.000Z")
	duration := 3 * time.Second
	err = error(nil)
	file := test.NewFile()
	b.SendFileImportEvent(ctx, api, duration, &err, file.FileKey, testStats(file.OpenedAt(), now))

	// Assert
	require.Equal(t, 1, transport.GetCallCountInfo()["POST /v2/storage/events"])
}

func testStats(firstAt, lastAt utctime.UTCTime) statistics.Value {
	return statistics.Value{
		SlicesCount:      1,
		FirstRecordAt:    firstAt,
		LastRecordAt:     lastAt,
		RecordsCount:     123,
		UncompressedSize: 100 * datasize.MB,
		CompressedSize:   50 * datasize.MB,
		StagingSize:      25 * datasize.MB,
	}
}

func registerOkResponder(t *testing.T, transport *httpmock.MockTransport, capturedBody *string) {
	t.Helper()
	transport.RegisterResponder(http.MethodPost, "/v2/storage/events", func(req *http.Request) (*http.Response, error) {
		reqBytes, err := httputil.DumpRequest(req, true)
		_, rawBody, _ := bytes.Cut(reqBytes, []byte("\r\n\r\n")) // headers and body are separated by an empty line
		require.NoError(t, err)

		var prettyBody bytes.Buffer
		require.NoError(t, json.Indent(&prettyBody, rawBody, "", "  "))

		*capturedBody = prettyBody.String()

		return httpmock.NewJsonResponderOrPanic(http.StatusCreated, map[string]any{"id": "12345"})(req)
	})
}

func registerErrorResponder(t *testing.T, transport *httpmock.MockTransport) {
	t.Helper()
	errResponse := httpmock.NewJsonResponderOrPanic(http.StatusForbidden, &keboola.StorageError{Message: "some error"})
	transport.RegisterResponder(http.MethodPost, "/v2/storage/events", errResponse)
}
