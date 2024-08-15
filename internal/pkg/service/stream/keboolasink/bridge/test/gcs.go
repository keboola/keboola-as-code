package test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
)

// TODO: not working.
func MockGCSBucket(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()

	transport.RegisterResponder(
		http.MethodGet,
		"http://testing/upload/storage/v1/b/b1/o?alt=json&name=testingmanifest&prettyPrint=false&projection=full&uploadType=multipart",
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusNotFound, &keboola.StorageError{ErrCode: "storage.buckets.notFound"})
		},
	)

	// Create bucket - ok
	expectedQuery := url.Values{
		"alt":         []string{"json"},
		"name":        []string{"testingmanifest"},
		"prettyPrint": []string{"false"},
		"projection":  []string{"full"},
		"uploadType":  []string{"multipart"},
	}
	transport.RegisterResponderWithQuery(
		http.MethodPost,
		"http://testing/upload/storage/v1/b/b1/o",
		expectedQuery,
		func(request *http.Request) (*http.Response, error) {
			return httpmock.NewJsonResponse(http.StatusOK, "{}")
		},
	)

	transport.RegisterNoResponder(httpmock.NewNotFoundResponder(tb.Fatal))
}

func GCSServer(tb testing.TB) string {
	tb.Helper()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// fmt.Println("server", r.URL)
		fmt.Fprintln(w, "{}")
		// w.WriteHeader(http.StatusOK)
	}))
	tb.Cleanup(func() {
		ts.Close()
	})

	return ts.URL[7:]
}
