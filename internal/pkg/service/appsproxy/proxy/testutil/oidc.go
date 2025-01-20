package testutil

import (
	"net"
	"strconv"
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

func StartOIDCProviderServer(t *testing.T, pm server.PortManager) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.NewServer(nil)
	if err != nil {
		panic("unable to open mockoidc server" + err.Error())
	}

	port := pm.GetFreePort()
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(int64(port), 10))
	for err != nil {
		port = pm.GetFreePort()
		ln, err = net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(int64(port), 10))
	}

	require.NoError(t, m.Start(ln, nil))
	return m
}
