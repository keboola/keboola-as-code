package test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MockBucketStorageAPICalls(t *testing.T, branchKey key.BranchKey, transport *httpmock.MockTransport) {
	t.Helper()

	// Get bucket - not found
	checkedBuckets := make(map[keboola.BucketID]bool)
	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("=~/v2/storage/branch/%s/buckets/.+", branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			parts := strings.Split(request.URL.String(), "/")
			bucketID := keboola.MustParseBucketID(parts[len(parts)-1])
			checkedBuckets[bucketID] = true
			return httpmock.NewJsonResponse(http.StatusOK, &keboola.StorageError{ErrCode: "storage.buckets.notFound"})
		},
	)

	// Create bucket - ok
	transport.RegisterResponder(
		http.MethodPost,
		fmt.Sprintf("/v2/storage/branch/%s/buckets", branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			dataBytes, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}

			data := make(map[string]any)
			if err := json.Decode(dataBytes, &data); err != nil {
				return nil, err
			}

			// Before POST, we expect GET request, to check bucket existence
			bucketID := keboola.MustParseBucketID(fmt.Sprintf("%s.c-%s", data["stage"], strings.TrimPrefix(data["name"].(string), "c-")))
			if !checkedBuckets[bucketID] {
				return nil, errors.Errorf(`unexpected order of requests, before creating the bucket "%s" via POST, it should be checked whether it exists via GET`, bucketID)
			}

			return httpmock.NewJsonResponse(http.StatusOK, &keboola.Bucket{
				BucketKey: keboola.BucketKey{
					BranchID: branchKey.BranchID,
					BucketID: bucketID,
				},
			})
		},
	)
}
