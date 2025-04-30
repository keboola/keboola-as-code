package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/storage_file_upload/gcs"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"go.uber.org/atomic"
)

func MockFileStorageAPICalls(tb testing.TB, clk clockwork.Clock, transport *httpmock.MockTransport) {
	tb.Helper()

	fileID := atomic.NewInt32(1000)

	// Mocked file prepare resource endpoint
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/branch/[0-9]+/files/prepare$`,
		func(request *http.Request) (*http.Response, error) {
			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			return httpmock.NewJsonResponse(http.StatusOK, &keboola.FileUploadCredentials{
				File: keboola.File{
					FileKey: keboola.FileKey{
						BranchID: branchID,
						FileID:   keboola.FileID(fileID.Inc()),
					},
					Provider: gcs.Provider,
				},
				GCSUploadParams: &gcs.UploadParams{
					Path: gcs.Path{
						Key:    "testing",
						Bucket: "b1",
					},
					Credentials: gcs.Credentials{
						ExpiresIn: int(clk.Now().Add(time.Hour).Unix()),
					},
				},
				S3UploadParams: &s3.UploadParams{
					Path: s3.Path{
						Key:    "test",
						Bucket: "b1",
					},
					Credentials: s3.Credentials{
						Expiration: iso8601.Time{Time: clk.Now().Add(time.Hour)},
					},
				},
			})
		},
	)

	// Mocked events call
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/events$`,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.Event{
				ID:          "123",
				ComponentID: "123",
				Message:     "abc",
				Type:        "event",
				Duration:    0,
			})
		},
	)

	// Mocked file delete endpoint
	transport.RegisterResponder(
		http.MethodDelete,
		`=~/v2/storage/branch/[0-9]+/files/\d+$$`,
		httpmock.NewStringResponder(http.StatusNoContent, ""),
	)
}
