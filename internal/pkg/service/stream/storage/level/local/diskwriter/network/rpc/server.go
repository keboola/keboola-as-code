package rpc

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"net"
	"sync"

	"google.golang.org/grpc"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc/pb"
)

type NetworkFileServer struct {
	pb.UnimplementedNetworkFileServer
	volumes *diskwriter.Volumes

	lock      sync.Mutex
	idCounter uint64
	writers   map[uint64]diskwriter.Writer
}

func NewNetworkFileServer(volumes *diskwriter.Volumes) *NetworkFileServer {
	return &NetworkFileServer{
		volumes: volumes,
		writers: make(map[uint64]diskwriter.Writer),
	}
}

func (s *NetworkFileServer) Serve(listener net.Listener) error {
	srv := grpc.NewServer()
	pb.RegisterNetworkFileServer(srv, s)
	return srv.Serve(listener)
}

func (s *NetworkFileServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	sliceKey := sliceKeyForm(req.SliceKey)

	vol, err := s.volumes.Collection().Volume(sliceKey.VolumeID)
	if err != nil {
		return nil, err
	}

	vol.OpenWriter()

	s.lock.Lock()
	defer s.lock.Unlock()
	s.idCounter++
	fileID := s.idCounter
	s.writers[fileID] = nil

	return &pb.OpenResponse{FileId: fileID}, nil
}

func (s *NetworkFileServer) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}

	n, err := w.Write(ctx, req.Data)
	if err != nil {
		return nil, err
	}

	return &pb.WriteResponse{N: int64(n)}, nil
}

func (s *NetworkFileServer) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.FlushResponse, error) {
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}
	return nil, w.Flush(ctx)
}

func (s *NetworkFileServer) Sync(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}
	return nil, w.Sync(ctx)
}

func (s *NetworkFileServer) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	defer func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.writers, req.FileId)
	}()
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}
	return nil, w.Close(ctx)
}

func (s *NetworkFileServer) writer(id uint64) (diskwriter.Writer, error) {
	w, found := s.writers[id]
	if !found {
		return nil, nil
	}
	return w, nil
}

func sliceKeyForm(k *pb.SliceKey) model.SliceKey {
	return model.SliceKey{
		FileVolumeKey: model.FileVolumeKey{
			FileKey: model.FileKey{
				SinkKey: key.SinkKey{
					SourceKey: key.SourceKey{
						BranchKey: key.BranchKey{
							ProjectID: keboola.ProjectID(k.ProjectId),
							BranchID:  keboola.BranchID(k.BranchId),
						},
						SourceID: key.SourceID(k.SourceId),
					},
					SinkID: key.SinkID(k.SinkId),
				},
			},
			VolumeID: volume.ID(k.VolumeId),
		},
		SliceID: model.SliceID{
			OpenedAt: utctime.From(k.SliceId.AsTime()),
		},
	}
}
