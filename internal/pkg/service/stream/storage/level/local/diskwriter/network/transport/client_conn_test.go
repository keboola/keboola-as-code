package transport_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport/tcp"
)

// A refused dial is retried and logged as info, the node itself did not fail.
func TestClientConnection_RefusedDialIsInfo(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	cfg := network.NewConfig()
	cfg.Listen = "localhost:0" // use a random port
	tr := tcp.New(cfg)
	cfg.Transport = tr.Type()

	// Reserve a port and release it, so nothing listens there
	srv, err := transport.Listen(log.NewDebugLogger(), "srv", cfg, tr)
	require.NoError(t, err)
	addr := srv.Addr().String()
	require.NoError(t, srv.Close())

	clientLogger := log.NewDebugLogger()
	client := transport.NewClient(clientLogger, cfg, "client-node", tr)
	_, err = client.OpenConnection(ctx, "srv", addr)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return strings.Contains(clientLogger.InfoMessages(), "disk writer client cannot connect to")
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, client.Close())
	clientLogger.AssertNoErrorMessage(t)
}
