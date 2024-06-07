package test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MockBucketStorageAPICalls(t *testing.T, transport *httpmock.MockTransport) {
	t.Helper()
	lock := &sync.Mutex{}

	// Get bucket - not found
	checkedBuckets := make(map[keboola.BucketKey]bool)
	existingBuckets := make(map[keboola.BucketKey]keboola.Bucket)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/[0-9]+/buckets/[a-z0-9\.\-]+$`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			bucketID, err := extractBucketIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			bucketKey := keboola.BucketKey{BranchID: branchID, BucketID: bucketID}

			if bucket, ok := existingBuckets[bucketKey]; ok {
				return httpmock.NewJsonResponse(http.StatusOK, bucket)
			}

			checkedBuckets[bucketKey] = true
			return httpmock.NewJsonResponse(http.StatusNotFound, &keboola.StorageError{ErrCode: "storage.buckets.notFound"})
		},
	)

	// Create bucket - ok
	transport.RegisterResponder(
		http.MethodPost,
		"=~/v2/storage/branch/[0-9]+/buckets$",
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

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
			bucketKey := keboola.BucketKey{BranchID: branchID, BucketID: bucketID}
			if !checkedBuckets[bucketKey] {
				return nil, errors.Errorf(`unexpected order of requests, before creating the bucket "%s" via POST, it should be checked whether it exists via GET`, bucketID)
			}

			bucket := keboola.Bucket{
				BucketKey: bucketKey,
			}
			existingBuckets[bucketKey] = bucket
			return httpmock.NewJsonResponse(http.StatusOK, bucket)
		},
	)
}
