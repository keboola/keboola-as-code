package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	local "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc/pb"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type NetworkFileServer struct {
	pb.UnimplementedNetworkFileServer

	logger     log.Logger
	volumes    *diskwriter.Volumes
	volumesMap map[volume.ID]bool

	lock      sync.Mutex
	idCounter uint64
	writers   map[uint64]diskwriter.Writer
}

type serverDependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	Volumes() *diskwriter.Volumes
	EtcdClient() *etcd.Client
	StorageRepository() *storageRepo.Repository
}

func StartNetworkFileServer(d serverDependencies, nodeID, hostname string, cfg local.Config) error {
	f := &NetworkFileServer{
		logger:     d.Logger().WithComponent("storage.node.writer.network-file"),
		volumes:    d.Volumes(),
		volumesMap: make(map[volume.ID]bool),
		writers:    make(map[uint64]diskwriter.Writer),
	}

	// Create volumes map, to quick check, if the volume is managed by the node
	for _, vol := range f.volumes.Collection().All() {
		f.volumesMap[vol.ID()] = true
	}

	// Listen for network connections
	listener, err := transport.Listen(f.logger, nodeID, cfg.Writer.Network)
	if err != nil {
		return err
	}
	d.Process().OnShutdown(func(ctx context.Context) {
		_ = listener.Close()
	})

	// Graceful shutdown
	ctx := context.Background()
	d.Process().OnShutdown(func(_ context.Context) {
		f.logger.Info(ctx, "closing network file server")

		// Stop network listener
		if err := listener.Close(); err != nil {
			f.logger.Error(ctx, err.Error())
		}

		// Close active writers
		f.closeWriters(ctx)

		f.logger.Info(ctx, "closed network file server")
	})

	// Register volumes to database
	nodeAddress := volume.RemoteAddr(fmt.Sprintf("%s:%s", hostname, listener.Port()))
	err = registration.RegisterVolumes(cfg.Volume.Registration, d, nodeID, nodeAddress, d.Volumes().Collection(), d.StorageRepository().Volume().RegisterWriterVolume)
	if err != nil {
		return err
	}

	// Start server
	d.Process().Add(func(shutdown servicectx.ShutdownFn) {
		shutdown(context.Background(), f.serve(listener))
	})

	return nil
}

func (s *NetworkFileServer) serve(listener net.Listener) error {
	srv := grpc.NewServer(
		grpc.SharedWriteBuffer(true),
	)
	pb.RegisterNetworkFileServer(srv, s)
	return srv.Serve(listener)
}

func (s *NetworkFileServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	var data sliceData
	if err := json.Decode(req.SliceDataJson, &data); err != nil {
		return nil, err
	}

	// Open volume
	vol, err := s.volumes.Collection().Volume(data.SliceKey.VolumeID)
	if err != nil {
		return nil, err
	}

	// Open writer
	w, err := vol.OpenWriter(req.SourceNodeId, data.SliceKey, data.LocalStorage)
	if err != nil {
		return nil, err
	}

	// Generate file ID for future calls and store reference to the writer
	s.lock.Lock()
	defer s.lock.Unlock()
	s.idCounter++
	fileID := s.idCounter
	s.writers[fileID] = w

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
	s.lock.Lock()
	defer s.lock.Unlock()

	w, found := s.writers[id]
	if !found {
		return nil, errors.Errorf("disk writer %d not found", id)
	}

	return w, nil
}

func (s *NetworkFileServer) closeWriters(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.logger.Infof(ctx, "closing %d disk writers", len(s.writers))

	wg := &sync.WaitGroup{}
	for _, id := range maps.Keys(s.writers) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := s.writers[id]
			delete(s.writers, id)
			if err := w.Close(ctx); err != nil {
				s.logger.Errorf(ctx, "cannot close disk writer %q: %s", w.SliceKey(), err)
			}
		}()
	}
	wg.Wait()
}
