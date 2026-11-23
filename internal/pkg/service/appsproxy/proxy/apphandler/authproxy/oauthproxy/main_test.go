package oauthproxy

import (
	"net/http"
	"testing"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/requests"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
)

func TestMain(m *testing.M) {
	requests.DefaultHTTPClient = &http.Client{Transport: testutil.IDPTransport()}
	m.Run()
}
