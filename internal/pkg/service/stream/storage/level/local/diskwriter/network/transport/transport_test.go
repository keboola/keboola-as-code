package transport_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
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

	cfg := network.NewConfig()
	cfg.Transport = protocol
	cfg.Listen = "localhost:0" // use a random port
	cfg.StreamOpenTimeout = 15 * time.Second
	cfg.StreamCloseTimeout = 15 * time.Second
	cfg.StreamWriteTimeout = 15 * time.Second
	cfg.ShutdownTimeout = 30 * time.Second
	cfg.KeepAliveInterval = 30 * time.Second // to not interfere with the test

	// Start server
	srvLogger := log.NewDebugLogger()
	srvLogger.ConnectTo(os.Stdout)
	srv, err := transport.Listen(srvLogger, "server-node", cfg)
	require.NoError(t, err)
	addr := srv.Addr().String()

	// Stream server handler
	var receivedLock sync.Mutex
	var received []string
	receivedDone := make(chan struct{}, 2)
	go func() {
		for {
			// Accept stream
			stream, err := srv.Accept()
			if errors.Is(err, io.ErrClosedPipe) {
				return
			}
			assert.NoError(t, err)

			// Read all
			b, err := io.ReadAll(stream)
			assert.NoError(t, err)

			// Close stream
			assert.NoError(t, stream.Close())

			// Collect result
			receivedLock.Lock()
			received = append(received, string(b))
			receivedLock.Unlock()
			receivedDone <- struct{}{}
		}
	}()

	// Setup client
	clientLogger := log.NewDebugLogger()
	clientLogger.ConnectTo(os.Stdout)
	client, err := transport.NewClient(clientLogger, cfg, "client-node")
	require.NoError(t, err)
	conn, err := client.OpenConnectionOrErr(ctx, "srv", addr)
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

	// Wait 2x for server handler
	for range 2 {
		select {
		case <-ctx.Done():
			assert.Fail(t, "timeout")
		case <-receivedDone:
		}
	}
	sort.Strings(received)
	assert.Equal(t, []string{"bar", "foo"}, received)

	shutdown(t, srv, client, srvLogger, clientLogger)
}

func testTransportBiggerData(t *testing.T, protocol network.TransportProtocol) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := network.NewConfig()
	cfg.Transport = protocol
	cfg.Listen = "localhost:0" // use a random port
	cfg.StreamOpenTimeout = 15 * time.Second
	cfg.StreamCloseTimeout = 15 * time.Second
	cfg.StreamWriteTimeout = 15 * time.Second
	cfg.ShutdownTimeout = 30 * time.Second
	cfg.KeepAliveInterval = 30 * time.Second // to not interfere with the test

	dataSize := 2 * datasize.MB
	data := []byte(strings.Repeat(".", int(dataSize.Bytes())))

	// Start Setup
	srvLogger := log.NewDebugLogger()
	srv, err := transport.Listen(srvLogger, "server-node", cfg)
	require.NoError(t, err)
	addr := srv.Addr().String()

	// Stream server handler
	receivedDone := make(chan struct{}, 1)
	go func() {
		for {
			// Accept stream
			stream, err := srv.Accept()
			if errors.Is(err, io.ErrClosedPipe) {
				return
			}
			assert.NoError(t, err)

			// Read all
			b, err := io.ReadAll(stream)
			if assert.Len(t, b, len(data)) {
				assert.Equal(t, data, b)
			}
			assert.NoError(t, err)

			// Close stream
			assert.NoError(t, stream.Close())

			receivedDone <- struct{}{}
		}
	}()

	// Setup client
	clientLogger := log.NewDebugLogger()
	client, err := transport.NewClient(clientLogger, cfg, "client-node")
	require.NoError(t, err)
	conn, err := client.OpenConnectionOrErr(ctx, "srv", addr)
	require.NoError(t, err)

	// Open stream and send data
	s, err := conn.OpenStream()
	require.NoError(t, err)
	startTime := time.Now()
	_, err = io.Copy(s, bytes.NewReader(data))
	assert.NoError(t, err)
	require.NoError(t, s.Close())
	t.Logf(`%s: write duration: %s`, protocol, time.Since(startTime).String())

	// Wait for server handler
	select {
	case <-ctx.Done():
		assert.Fail(t, "timeout")
	case <-receivedDone:
	}

	shutdown(t, srv, client, srvLogger, clientLogger)
}

func shutdown(t *testing.T, srv *transport.Server, client *transport.Client, srvLogger, clientLogger log.DebugLogger) {
	t.Helper()

	// Don't start shutdown, before the successful connection is logged
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		srvLogger.AssertJSONMessages(c, `{"message":"accepted connection from \"%s\" to \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		clientLogger.AssertJSONMessages(c, `{"message":"disk writer client connected from \"%s\" to \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Close client
	require.NoError(t, client.Close())

	// Close server
	require.NoError(t, srv.Close())

	// Check client logs
	clientLogger.AssertJSONMessages(t, `
{"level":"info","message":"disk writer client connected from \"%s\" to \"srv\" - \"%s\""}
{"level":"info","message":"closing disk writer client"}
{"level":"info","message":"closing %d connections"}
{"level":"info","message":"closed disk writer client"}
`)

	// Check server logs
	srvLogger.AssertJSONMessages(t, `
{"level":"info","message":"disk writer listening on \"%s\""}
{"level":"info","message":"accepted connection from \"%s\" to \"%s\""}
{"level":"info","message":"closing disk writer server"}
{"level":"info","message":"waiting 30s for %d streams"}
{"level":"info","message":"waiting for streams done"}
{"level":"info","message":"closing %d streams"}
{"level":"info","message":"closing %d sessions"}
{"level":"info","message":"closed disk writer server"}
`)
}
