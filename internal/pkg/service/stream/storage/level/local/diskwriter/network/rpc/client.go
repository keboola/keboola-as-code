package rpc

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc/pb"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type networkFile struct {
	conn   *grpc.ClientConn
	rpc    pb.NetworkFileClient
	fileID uint64
}

func OpenNetworkFile(ctx context.Context, conn *transport.ClientConnection, sliceKey model.SliceKey) (encoding.NetworkFile, error) {
	// Create gRPC client
	clientConn, err := grpc.NewClient("", grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		stream, err := conn.OpenStream()
		if err != nil {
			return nil, errors.PrefixErrorf(err, `cannot open stream to the volume "%s" for the slice "%s"`, sliceKey.VolumeID.String(), sliceKey.String())
		}
		return stream, nil
	}))
	if err != nil {
		return nil, err
	}

	// Open remote file
	f := &networkFile{conn: clientConn, rpc: pb.NewNetworkFileClient(clientConn)}
	if err := f.open(ctx, sliceKey); err != nil {
		_ = f.conn.Close()
	}

	return f, nil
}

func (f *networkFile) open(ctx context.Context, k model.SliceKey) error {
	resp, err := f.rpc.Open(ctx, &pb.OpenRequest{
		SliceKey: &pb.SliceKey{
			ProjectId: int64(k.ProjectID),
			BranchId:  int64(k.BranchID),
			SourceId:  k.SourceID.String(),
			SinkId:    k.SinkID.String(),
			FileId:    timestamppb.New(k.FileID.OpenedAt.Time()),
			VolumeId:  k.VolumeID.String(),
			SliceId:   timestamppb.New(k.SliceID.OpenedAt.Time()),
		},
	})
	f.fileID = resp.FileId
	return err
}

// IsReady returns true if the underlying network is working.
func (f *networkFile) IsReady() bool {
	s := f.conn.GetState()
	return s == connectivity.Idle || s == connectivity.Ready
}

// Write bytes to the buffer in the disk writer node.
func (f *networkFile) Write(p []byte) (int, error) {
	resp, err := f.rpc.Write(context.Background(), &pb.WriteRequest{FileId: f.fileID, Data: p})
	if err != nil {
		return 0, err
	}
	return int(resp.N), nil
}

// Flush buffered bytes to the OS disk cache,
// so only completed parts are passed to the disk.
func (f *networkFile) Flush(ctx context.Context) error {
	_, err := f.rpc.Flush(ctx, &pb.FlushRequest{FileId: f.fileID})
	return err
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
