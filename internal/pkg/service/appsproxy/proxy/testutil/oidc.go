package testutil

import (
	"net"
	"strconv"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"
)

func StartOIDCProviderServer(t *testing.T, pm server.PortManager) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.NewServer(nil)
	if err != nil {
		panic("unable to open mockoidc server" + err.Error())
	}

	port := pm.GetFreePort()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	for err != nil {
		port = pm.GetFreePort()
		ln, err = net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(int64(port), 10))
	}

	m.Start(ln, nil)
	require.NoError(t, err)

	return m
}
