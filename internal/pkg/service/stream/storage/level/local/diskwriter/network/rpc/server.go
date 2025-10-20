package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type NetworkFileServer struct {
	pb.UnimplementedNetworkFileServer

	telemetry  telemetry.Telemetry
	logger     log.Logger
	volumes    *diskwriter.Volumes
	volumesMap map[volume.ID]bool

	terminating chan struct{}

	lock      sync.Mutex
	idCounter uint64
	writers   map[uint64]diskwriter.Writer
}

type serverDependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	Volumes() *diskwriter.Volumes
	EtcdClient() *etcd.Client
	StorageRepository() *storageRepo.Repository
}

func StartNetworkFileServer(d serverDependencies, nodeID, hostname string, cfg local.Config) error {
	f := &NetworkFileServer{
		telemetry:   d.Telemetry(),
		logger:      d.Logger().WithComponent("storage.node.writer.rpc"),
		volumes:     d.Volumes(),
		volumesMap:  make(map[volume.ID]bool),
		terminating: make(chan struct{}),
		writers:     make(map[uint64]diskwriter.Writer),
	}

	// Create volumes map, to quick check, if the volume is managed by the node
	for _, vol := range f.volumes.Collection().All() {
		f.volumesMap[vol.ID()] = true
	}

	// Init transport protocol
	tr, err := transport.NewProtocol(cfg.Writer.Network)
	if err != nil {
		return err
	}

	// Listen for network connections
	listener, err := transport.Listen(f.logger, nodeID, cfg.Writer.Network, tr)
	if err != nil {
		return err
	}

	// Register volumes to database, so source nodes know about this disk writer node.
	nodeAddress := volume.RemoteAddr(fmt.Sprintf("%s:%s", hostname, listener.Port()))
	err = registration.RegisterVolumes(cfg.Volume.Registration, d, nodeID, nodeAddress, d.Volumes().Collection(), d.StorageRepository().Volume().RegisterWriterVolume)
	if err != nil {
		return err
	}

	// Graceful shutdown
	ctx := context.Background()
	d.Process().OnShutdown(func(_ context.Context) {
		f.logger.Info(ctx, "closing network file server")

		// Gracefully close active writers
		f.terminate(ctx)

		// Stop network listener
		if err := listener.Close(); err != nil {
			f.logger.Error(ctx, err.Error())
		}

		f.logger.Info(ctx, "closed network file server")
	})

	// Start server
	d.Process().Add(func(shutdown servicectx.ShutdownFn) {
		shutdown(context.Background(), f.serve(listener))
	})

	return nil
}

func (s *NetworkFileServer) serve(listener net.Listener) error {
	srv := grpc.NewServer(
		grpc.SharedWriteBuffer(true),
		grpc.StatsHandler(
			otelgrpc.NewClientHandler(
				otelgrpc.WithMeterProvider(s.telemetry.MeterProvider()),
				otelgrpc.WithTracerProvider(s.telemetry.TracerProvider()),
			),
		),
	)
	pb.RegisterNetworkFileServer(srv, s)
	return srv.Serve(listener)
}

func (s *NetworkFileServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	if s.isTerminating() {
		return nil, errors.New("disk writer node is terminating")
	}

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
	w, err := vol.OpenWriter(req.SourceNodeId, data.SliceKey, data.LocalStorage, data.WithBackup)
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

func (s *NetworkFileServer) KeepAliveStream(req *pb.KeepAliveStreamRequest, stream pb.NetworkFile_KeepAliveStreamServer) error {
	select {
	case <-s.terminating:
		return stream.Send(&pb.KeepAliveStreamResponse{})

	case <-stream.Context().Done():
		// The client is gone, remove reference for the writer, and do graceful close
		s.lock.Lock()
		w, ok := s.writers[req.FileId]
		s.lock.Unlock()
		if !ok {
			return nil
		}

		ctx, cancel := context.WithTimeoutCause(context.Background(), 30*time.Second, errors.New("writer close timout"))
		defer cancel()
		return w.Close(ctx)
	}
}

func (s *NetworkFileServer) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}

	n, err := w.Write(ctx, req.Aligned, req.Data)
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
	w, err := s.writer(req.FileId)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	delete(s.writers, req.FileId)
	s.lock.Unlock()

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

func (s *NetworkFileServer) isTerminating() bool {
	select {
	case <-s.terminating:
		return true
	default:
		return false
	}
}

func (s *NetworkFileServer) terminate(ctx context.Context) {
	// Prevent opening of a new writer, see Open method.
	// It also notifies source nodes about server termination, see KeepAliveStream method.
	close(s.terminating)

	// Notified source nodes should close their pipelines, and finally the network file client, see Close method.
	s.logger.Infof(ctx, "waiting for close of %d disk writers by source nodes", len(s.writers))

	// Wait until all writers are closed
	s.waitForWritersClosing(ctx)

	// Force close remaining writers
	s.lock.Lock()
	defer s.lock.Unlock()
	if l := len(s.writers); l > 0 {
		s.logger.Errorf(ctx, "force closing %d disk writers", len(s.writers))
		wg := &sync.WaitGroup{}
		for _, w := range s.writers {
			wg.Go(func() {
				if err := w.Close(ctx); err != nil {
					s.logger.Errorf(ctx, "cannot close disk writer %q: %s", w.SliceKey(), err)
				}
			})
		}
		wg.Wait()
	}
}

func (s *NetworkFileServer) waitForWritersClosing(ctx context.Context) {
	ctx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, errors.New("writers closing timeout"))
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
			// try again
		}

		s.lock.Lock()
		l := len(s.writers)
		s.lock.Unlock()
		if l == 0 {
			s.logger.Infof(ctx, "all writers have been gracefully closed")
			return
		}
	}
}
