package testutil_test

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
)

type osPorts struct{}

func (osPorts) GeneratePorts(context.Context) {}
func (osPorts) GetFreePort() int              { return 0 }

func TestAppServer_DownKeepsThePortAndFailsRequests(t *testing.T) {
	t.Parallel()
	appServer := testutil.StartAppServer(t, osPorts{})
	t.Cleanup(appServer.Close)

	appServer.Down()

	var lc net.ListenConfig
	l, err := lc.Listen(t.Context(), "tcp", appServer.Listener.Addr().String())
	if err == nil {
		_ = l.Close()
	}
	require.Error(t, err, "the port of a down app server must stay taken")

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, appServer.URL+"/", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	assert.Error(t, err)
}
