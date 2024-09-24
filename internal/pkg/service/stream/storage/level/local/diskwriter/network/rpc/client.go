package rpc

import (
	"context"
	"io"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc/pb"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type networkFile struct {
	conn *grpc.ClientConn
	rpc  pb.NetworkFileClient

	sliceKey model.SliceKey
	fileID   uint64

	cancel context.CancelFunc
	closed <-chan struct{}
}

func OpenNetworkFile(ctx context.Context, logger log.Logger, sourceNodeID string, conn *transport.ClientConnection, sliceKey model.SliceKey, slice localModel.Slice, onServerTermination func(ctx context.Context, cause string)) (encoding.NetworkOutput, error) {
	logger = logger.WithComponent("rpc")

	// Use transport layer with multiplexer for connection
	dialer := func(_ context.Context, _ string) (net.Conn, error) {
		stream, err := conn.OpenStream()
		if err != nil {
			return nil, errors.PrefixErrorf(err, `cannot open stream to the volume "%s" for the slice "%s"`, sliceKey.VolumeID.String(), sliceKey.String())
		}
		return stream, nil
	}

	// https://grpc.io/docs/guides/retry/
	// https://grpc.io/docs/guides/service-config/
	serviceConfig := `
{
	"methodConfig": [
		{
			"name": [
				{}
			],
			"waitForReady": true,
			"timeout": "10s",
			"retryPolicy": {
				"MaxAttempts": 5,
				"InitialBackoff": ".01s",
				"MaxBackoff": ".05s",
				"BackoffMultiplier": 2.0,
				"RetryableStatusCodes": [
					"UNAVAILABLE"
				]
			}
		},
		{
			"name": [
				{
					"service": "pb.NetworkFile",
					"method": "WaitForServerTermination"
				}
			],
			"timeout": null
		}
	]
}`

	// Create gRPC client
	clientConn, err := grpc.NewClient(
		"127.0.0.1",
		grpc.WithSharedWriteBuffer(true),
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	f := &networkFile{
		conn:     clientConn,
		rpc:      pb.NewNetworkFileClient(clientConn),
		sliceKey: sliceKey,
		cancel:   cancel,
		closed:   ctx.Done(),
	}

	// Try to open remote file
	if err := f.open(ctx, sourceNodeID, slice); err != nil {
		_ = clientConn.Close()
		return nil, err
	}

	// Listen from termination notifications from the network file server side
	// In order for the server to contact the client, we must use a stream.
	termStream, err := f.rpc.WaitForServerTermination(context.WithoutCancel(ctx), &pb.WaitForServerTerminationRequest{FileId: f.fileID})
	if err != nil {
		_ = clientConn.Close()
		return nil, err
	}
	go func() {
		// It is expected to receive only one message, `io.EOF` or `message` that the termination is done
		if _, err := termStream.Recv(); err != nil {
			if errors.Is(err, io.EOF) {
				onServerTermination(ctx, "remote server shutdown")
				return
			}

			if s, ok := status.FromError(err); !ok || s.Code() != codes.Canceled {
				logger.Errorf(ctx, "error when waiting for network file server termination: %s", err)
			}
		}

		if err := termStream.CloseSend(); err != nil {
			if s, ok := status.FromError(err); !ok || s.Code() != codes.Canceled {
				logger.Errorf(ctx, "rpc close send error: %s", err)
			}
		}

		onServerTermination(ctx, "remote server shutdown")
	}()

	return f, nil
}

func (f *networkFile) open(ctx context.Context, sourceNodeID string, slice localModel.Slice) error {
	sliceJSON, err := json.Encode(sliceData{SliceKey: f.sliceKey, LocalStorage: slice}, false)
	if err != nil {
		return err
	}

	req := &pb.OpenRequest{
		SourceNodeId:  sourceNodeID,
		SliceDataJson: sliceJSON,
	}

	resp, err := f.rpc.Open(ctx, req)
	if err != nil {
		return errors.PrefixError(err, "network file client: rpc open error")
	}

	f.fileID = resp.FileId
	return nil
}

// IsReady returns true if the underlying network is working.
func (f *networkFile) IsReady() bool {
	if f.isClosed() {
		return false
	}
	s := f.conn.GetState()
	return s == connectivity.Idle || s == connectivity.Ready
}

// Write bytes to the buffer in the disk writer node.
func (f *networkFile) Write(ctx context.Context, aligned bool, p []byte) (int, error) {
	if f.isClosed() {
		return 0, errors.New("network file client: rpc write error: writer is closed")
	}

	resp, err := f.rpc.Write(ctx, &pb.WriteRequest{FileId: f.fileID, Aligned: aligned, Data: p})
	if err != nil {
		return 0, errors.PrefixError(err, "network file client: rpc write error")
	}

	return int(resp.N), nil
}

// Sync OS disk cache to the physical disk.
func (f *networkFile) Sync(ctx context.Context) error {
	if f.isClosed() {
		return errors.New("network file client: rpc sync error: writer is closed")
	}
	if _, err := f.rpc.Sync(ctx, &pb.SyncRequest{FileId: f.fileID}); err != nil {
		return errors.PrefixError(err, "network file client: rpc sync error")
	}
	return nil
}

// Close the underlying OS file and network connection.
func (f *networkFile) Close(ctx context.Context) error {
	if f.isClosed() {
		return errors.New("network file client close error: already closed")
	}

	// Close WaitForServerTermination stream
	f.cancel()

	// Close remote network file
	if _, err := f.rpc.Close(ctx, &pb.CloseRequest{FileId: f.fileID}); err != nil {
		return errors.PrefixError(err, "network file client: rpc close error")
	}

	// Close connection to the disk writer node
	if err := f.conn.Close(); err != nil {
		return errors.PrefixError(err, "network file client: close connection error")
	}

	return nil
}

func (f *networkFile) isClosed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}
