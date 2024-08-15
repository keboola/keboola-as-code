package test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
)

func MockS3Bucket(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()

	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/buckets/", "=~s3://"),
		// `=~s3`,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusNotFound, &keboola.StorageError{ErrCode: "storage.buckets.notFound"})
		},
	)

	// Create bucket - ok
	transport.RegisterResponder(
		http.MethodPut,
		fmt.Sprintf("%s/buckets/", "=~s3://"),
		// `=~s3`,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, "{}")
		},
	)

	transport.RegisterNoResponder(httpmock.NewNotFoundResponder(tb.Fatal))
}
