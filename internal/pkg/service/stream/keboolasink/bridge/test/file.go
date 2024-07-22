package test

import (
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"go.uber.org/atomic"
)

func MockFileStorageAPICalls(tb testing.TB, clk clock.Clock, transport *httpmock.MockTransport) {
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
				},
				S3UploadParams: &s3.UploadParams{
					Credentials: s3.Credentials{
						Expiration: iso8601.Time{Time: clk.Now().Add(time.Hour)},
					},
				},
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
