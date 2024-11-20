package test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"go.uber.org/atomic"
)

func MockImportAsyncAPICalls(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()

	jobID := atomic.NewInt32(321)
	jobStr := strconv.FormatInt(int64(jobID.Load()), 10)
	// Mocked import async resource endpoint
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/branch/[0-9]+/tables/in\.c-bucket\.my-table/import-async`,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.StorageJob{
				StorageJobKey: keboola.StorageJobKey{
					ID: keboola.StorageJobID(jobID.Load()),
				},
				Status:        "processing",
				URL:           "https://connection.keboola.com/v2/storage/jobs/" + jobStr,
				OperationName: "importFile",
			})
		},
	)
}

func MockProcessingJobStorageAPICalls(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()

	jobID := atomic.NewInt32(321)
	jobStr := strconv.FormatInt(int64(jobID.Load()), 10)
	// Mocked job inspection resource endpoint
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/jobs/`+jobStr,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.StorageJob{
				StorageJobKey: keboola.StorageJobKey{
					ID: keboola.StorageJobID(jobID.Load()),
				},
				Status:        "processing",
				URL:           "https://connection.keboola.com/v2/storage/jobs/" + jobStr,
				OperationName: "importFile",
			})
		},
	)
}

func MockSuccessJobStorageAPICalls(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()

	jobID := atomic.NewInt32(321)
	jobStr := strconv.FormatInt(int64(jobID.Load()), 10)
	// Mocked file prepare resource endpoint
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/jobs/`+jobStr,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.StorageJob{
				StorageJobKey: keboola.StorageJobKey{
					ID: keboola.StorageJobID(jobID.Load()),
				},
				Status:        "success",
				URL:           "https://connection.keboola.com/v2/storage/jobs/" + jobStr,
				OperationName: "importFile",
			})
		},
	)
}
