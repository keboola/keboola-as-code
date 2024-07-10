package transport_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/hashicorp/yamux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestTransportSmallData_TCP(t *testing.T) {
	t.Parallel()
	testTransportSmallData(t, network.TransportProtocolTCP)
}

func TestTransport_SmallData_KCP(t *testing.T) {
	t.Parallel()
	testTransportSmallData(t, network.TransportProtocolKCP)
}

func TestTransportBiggerData_TCP(t *testing.T) {
	t.Parallel()
	testTransportBiggerData(t, network.TransportProtocolTCP)
}

func TestTransportBiggerData_KCP(t *testing.T) {
	t.Parallel()
	testTransportBiggerData(t, network.TransportProtocolKCP)
}

func testTransportSmallData(t *testing.T, protocol network.TransportProtocol) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srvDeps, srvMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))
	clientDeps, clientMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))

	cfg := network.NewConfig()
	cfg.Transport = protocol
	cfg.Listen = "localhost:0" // use a random port
	cfg.StreamWriteTimeout = 30 * time.Second
	cfg.ShutdownTimeout = 30 * time.Second
	cfg.KeepAliveInterval = 30 * time.Second // to not interfere with the test

	// Stream server handler
	var lock sync.Mutex
	var received []string
	handler := func(ctx context.Context, stream *yamux.Stream) {
		b, err := io.ReadAll(stream)
		require.NoError(t, err)

		lock.Lock()
		defer lock.Unlock()
		received = append(received, string(b))
	}

	// Start Setup
	srv, err := transport.Listen(srvDeps, cfg, "server-node", handler)
	require.NoError(t, err)
	addr := srv.ListenAddr().String()

	// Setup client
	client, err := transport.NewClient(clientDeps, cfg, "client-node")
	require.NoError(t, err)
	conn, err := client.ConnectTo(addr)
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

	shutdown(t, clientDeps, srvDeps, clientMock.DebugLogger(), srvMock.DebugLogger())
}

func testTransportBiggerData(t *testing.T, protocol network.TransportProtocol) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srvDeps, srvMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))
	clientDeps, clientMock := dependencies.NewMockedServiceScope(t, commonDeps.WithCtx(ctx))

	cfg := network.NewConfig()
	cfg.Transport = protocol
	cfg.Listen = "localhost:0" // use a random port
	cfg.StreamWriteTimeout = 30 * time.Second
	cfg.ShutdownTimeout = 30 * time.Second
	cfg.KeepAliveInterval = 30 * time.Second // to not interfere with the test

	dataSize := 2 * datasize.MB
	data := []byte(strings.Repeat(".", int(dataSize.Bytes())))

	// Stream server handler
	handler := func(ctx context.Context, stream *yamux.Stream) {
		b, err := io.ReadAll(stream)
		if assert.Len(t, b, len(data)) {
			assert.Equal(t, data, b)
		}
		assert.NoError(t, err)
	}

	// Start Setup
	srv, err := transport.Listen(srvDeps, cfg, "server-node", handler)
	require.NoError(t, err)
	addr := srv.ListenAddr().String()

	// Setup client
	client, err := transport.NewClient(clientDeps, cfg, "client-node")
	require.NoError(t, err)
	conn, err := client.ConnectTo(addr)
	require.NoError(t, err)

	// Open stream and send data
	s, err := conn.OpenStream()
	require.NoError(t, err)
	startTime := time.Now()
	_, err = io.Copy(s, bytes.NewReader(data))
	assert.NoError(t, err)
	require.NoError(t, s.Close())
	t.Logf(`%s: write duration: %s`, protocol, time.Since(startTime).String())

	shutdown(t, clientDeps, srvDeps, clientMock.DebugLogger(), srvMock.DebugLogger())
}

func shutdown(t *testing.T, clientDeps, srvDeps dependencies.ServiceScope, clientLogger, srvLogger log.DebugLogger) {
	t.Helper()

	// Don't start shutdown, before the successful connection is logged
	assert.Eventually(t, func() bool {
		return srvLogger.CompareJSONMessages(`{"message":"accepted connection from \"127.0.0.1:%d\""}`) == nil
	}, 5*time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool {
		return clientLogger.CompareJSONMessages(`{"message":"disk writer client connected to \"127.0.0.1:%d\""}`) == nil
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown client
	clientDeps.Process().Shutdown(context.Background(), errors.New("bye bye"))
	clientDeps.Process().WaitForShutdown()

	// Shutdown server
	srvDeps.Process().Shutdown(context.Background(), errors.New("bye bye"))
	srvDeps.Process().WaitForShutdown()

	// Check client logs
	clientLogger.AssertJSONMessages(t, `
{"level":"info","message":"disk writer client connected to \"127.0.0.1:%d\"","component":"storage.node.writer.network.client"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"closing disk writer client","component":"storage.node.writer.network.client"}
{"level":"info","message":"closing %d streams","component":"storage.node.writer.network.client"}
{"level":"info","message":"closing %d sessions","component":"storage.node.writer.network.client"}
{"level":"info","message":"closed disk writer client","component":"storage.node.writer.network.client"}
{"level":"info","message":"exited"}
`)

	// Check server logs
	srvLogger.AssertJSONMessages(t, `
{"level":"info","message":"disk writer listening on \"127.0.0.1:%d\"","component":"storage.node.writer.network.server"}
{"level":"info","message":"accepted connection from \"127.0.0.1:%d\"","component":"storage.node.writer.network.server"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"closing disk writer server","component":"storage.node.writer.network.server"}
{"level":"info","message":"waiting 30s for %d streams","component":"storage.node.writer.network.server"}
{"level":"info","message":"waiting for streams done","component":"storage.node.writer.network.server"}
{"level":"info","message":"closing %d streams","component":"storage.node.writer.network.server"}
{"level":"info","message":"closing %d sessions","component":"storage.node.writer.network.server"}
{"level":"info","message":"closed disk writer server","component":"storage.node.writer.network.server"}
{"level":"info","message":"exited"}
`)
}
