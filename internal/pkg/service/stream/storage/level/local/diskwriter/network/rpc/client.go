package rpc

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
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
}

func OpenNetworkFile(ctx context.Context, sourceNodeID string, conn *transport.ClientConnection, sliceKey model.SliceKey, slice localModel.Slice) (encoding.NetworkOutput, error) {
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
	serviceConfig := `{
		"methodConfig": [{
		  "name": [{}],
		  "waitForReady": true,
          "timeout": "10s",
		  "retryPolicy": {
			  "MaxAttempts": 5,
			  "InitialBackoff": ".01s",
			  "MaxBackoff": ".05s",
			  "BackoffMultiplier": 2.0,
			  "RetryableStatusCodes": [ "UNAVAILABLE" ]
		  }
		}]}`

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

	// Try to open remote file
	f := &networkFile{conn: clientConn, rpc: pb.NewNetworkFileClient(clientConn), sliceKey: sliceKey}
	if err := f.open(ctx, sourceNodeID, slice); err != nil {
		_ = clientConn.Close()
		return nil, err
	}

	return f, nil
}

func (f *networkFile) open(ctx context.Context, sourceNodeID string, slice localModel.Slice) error {
	sliceJSON, err := json.Encode(sliceData{SliceKey: f.sliceKey, LocalStorage: slice}, false)
	if err != nil {
		return err
	}

	resp, err := f.rpc.Open(ctx, &pb.OpenRequest{
		SourceNodeId:  sourceNodeID,
		SliceDataJson: sliceJSON,
	})
	if err != nil {
		return err
	}

	f.fileID = resp.FileId
	return nil
}

// IsReady returns true if the underlying network is working.
func (f *networkFile) IsReady() bool {
	s := f.conn.GetState()
	return s == connectivity.Idle || s == connectivity.Ready
}

// Write bytes to the buffer in the disk writer node.
func (f *networkFile) Write(ctx context.Context, aligned bool, p []byte) (int, error) {
	// grpc.CallContentSubtype("pb.WriteRequest")
	resp, err := f.rpc.Write(ctx, &pb.WriteRequest{FileId: f.fileID, Aligned: aligned, Data: p})
	if err != nil {
		return 0, err
	}

	return int(resp.N), nil
}

// Sync OS disk cache to the physical disk.
func (f *networkFile) Sync(ctx context.Context) error {
	_, err := f.rpc.Sync(ctx, &pb.SyncRequest{FileId: f.fileID})
	return err
}

// Close the underlying OS file and network connection.
func (f *networkFile) Close(ctx context.Context) error {
	_, err := f.rpc.Close(ctx, &pb.CloseRequest{FileId: f.fileID})
	return err
}
