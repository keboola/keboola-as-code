package repository_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-client/pkg/keboola/storage_file_upload/s3"
	"github.com/relvacode/iso8601"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func mockStorageAPICalls(t *testing.T, clk clock.Clock, branchKey key.BranchKey, transport *httpmock.MockTransport) {
	t.Helper()

	fileID := atomic.NewInt32(1000)

	// Mocked file prepare resource endpoint
	transport.RegisterResponder(
		http.MethodPost,
		fmt.Sprintf("/v2/storage/branch/%d/files/prepare", branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.FileUploadCredentials{
				File: keboola.File{
					FileKey: keboola.FileKey{
						BranchID: branchKey.BranchID,
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
		fmt.Sprintf(`=~/v2/storage/branch/%d/files/\d+$`, branchKey.BranchID),
		httpmock.NewStringResponder(http.StatusNoContent, ""),
	)
}
