package transport_test

import (
	"context"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestServerAndClient(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srvDeps, srvMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))
	clientDeps, clientMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))

	cfg := network.NewConfig()
	cfg.Listen = "localhost:0"               // use a random port
	cfg.KeepAliveInterval = 30 * time.Second // to not interfere with the test

	// Stream server handler
	var lock sync.Mutex
	var received []string
	handler := func(ctx context.Context, stream *yamux.Stream) {
		bytes, err := io.ReadAll(stream)
		require.NoError(t, err)

		lock.Lock()
		defer lock.Unlock()
		received = append(received, string(bytes))
	}

	// Start Setup
	srv, err := transport.Listen(srvDeps, cfg, "server-node", handler)
	require.NoError(t, err)
	addr := srv.ListenAddr().String()

	// Setup client
	conn, err := transport.NewClient(clientDeps, cfg, "client-node").ConnectTo(addr)
	require.NoError(t, err)

	// Open streams
	s1, err := conn.OpenStream()
	require.NoError(t, err)
	s2, err := conn.OpenStream()
	require.NoError(t, err)

	// Send bytes to stream 1 and close streams
	_, err = s1.Write([]byte("foo"))
	require.NoError(t, err)
	require.NoError(t, s1.Close())

	// Send bytes to stream 2 and close stream
	_, err = s2.Write([]byte("bar"))
	require.NoError(t, err)
	require.NoError(t, s2.Close())

	// Shutdown client
	clientDeps.Process().Shutdown(ctx, errors.New("bye bye"))
	clientDeps.Process().WaitForShutdown()

	// Shutdown server
	srvDeps.Process().Shutdown(ctx, errors.New("bye bye"))
	srvDeps.Process().WaitForShutdown()

	// Check received data
	sort.Strings(received)
	assert.Equal(t, []string{"bar", "foo"}, received)

	// Check client logs
	clientMock.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"disk writer client connected to \"127.0.0.1:%d\"","component":"storage.node.writer.network.client"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"closing disk writer client","component":"storage.node.writer.network.client"}
{"level":"info","message":"closing 0 streams","component":"storage.node.writer.network.client"}
{"level":"info","message":"closing 1 sessions","component":"storage.node.writer.network.client"}
{"level":"info","message":"closed disk writer client","component":"storage.node.writer.network.client"}
{"level":"info","message":"exited"}
`)

	// Check server logs
	srvMock.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"disk writer listening on \"127.0.0.1:%d\"","component":"storage.node.writer.network.server"}
{"level":"info","message":"accepted connection from \"127.0.0.1:%d\"","component":"storage.node.writer.network.server"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"closing disk writer server","component":"storage.node.writer.network.server"}
{"level":"info","message":"waiting 5s for %d streams","component":"storage.node.writer.network.server"}
{"level":"info","message":"waiting for streams done","component":"storage.node.writer.network.server"}
{"level":"info","message":"closing 0 streams","component":"storage.node.writer.network.server"}
{"level":"info","message":"closing 1 sessions","component":"storage.node.writer.network.server"}
{"level":"info","message":"closed disk writer server","component":"storage.node.writer.network.server"}
{"level":"info","message":"exited"}
`)
}
