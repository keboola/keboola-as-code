package testutil

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// AppServerURL returns the URL of the app server for use as the upstream target.
func AppServerURL(t *testing.T, appServer *AppServer) *url.URL {
	t.Helper()

	u, err := url.Parse(appServer.URL)
	require.NoError(t, err)
	return u
}
