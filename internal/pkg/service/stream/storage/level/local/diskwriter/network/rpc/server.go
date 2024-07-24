package rpc

import (
	"context"
	"net"
	"sync"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc/pb"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	storage "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type NetworkFileServer struct {
	pb.UnimplementedNetworkFileServer

	logger     log.Logger
	volumes    *diskwriter.Volumes
	volumesMap map[volume.ID]bool

	// slices field contains in-memory snapshot of all opened storage file slices
	slices *etcdop.MirrorMap[storage.Slice, storage.SliceKey, *sliceData]

	lock      sync.Mutex
	idCounter uint64
	writers   map[uint64]diskwriter.Writer
}

type sliceData struct {
	SliceKey     storage.SliceKey
	State        storage.SliceState
	LocalStorage localModel.Slice
}

type serverDependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	Volumes() *diskwriter.Volumes
	StorageRepository() *storageRepo.Repository
}

func NewNetworkFileServer(d serverDependencies) (*NetworkFileServer, error) {
	f := &NetworkFileServer{
		logger:     d.Logger(),
		volumes:    d.Volumes(),
		volumesMap: make(map[volume.ID]bool),
		writers:    make(map[uint64]diskwriter.Writer),
	}

	// Create volumes mat, to quick check, if the volume is managed by the node
	for _, vol := range f.volumes.Collection().All() {
		f.volumesMap[vol.ID()] = true
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		f.logger.Info(ctx, "closing network file server")

		// Stop mirroring
		cancel()
		wg.Wait()

		// Close active writers
		f.closeWriters(ctx)

		f.logger.Info(ctx, "closed network file server")
	})

	// Start slices mirroring, only necessary data is saved
	{
		f.slices = etcdop.
			SetupMirrorMap[storage.Slice](
			d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, storage.LevelLocal, etcd.WithPrevKV()),
			func(key string, slice storage.Slice) storage.SliceKey {
				return slice.SliceKey
			},
			func(key string, slice storage.Slice) *sliceData {
				return &sliceData{
					SliceKey:     slice.SliceKey,
					State:        slice.State,
					LocalStorage: slice.LocalStorage,
				}
			},
		).
			WithFilter(func(event etcdop.WatchEvent[storage.Slice]) bool {
				// Mirror only slices from managed volumes
				return f.volumesMap[event.Value.VolumeID]
			}).
			BuildMirror()
		if err := <-f.slices.StartMirroring(ctx, wg, f.logger); err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (s *NetworkFileServer) Serve(listener net.Listener) error {
	srv := grpc.NewServer(
		grpc.SharedWriteBuffer(true),
	)
	pb.RegisterNetworkFileServer(srv, s)
	return srv.Serve(listener)
}

func (s *NetworkFileServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	sliceKey := sliceKeyFrom(req.SliceKey)

	// Open volume
	vol, err := s.volumes.Collection().Volume(sliceKey.VolumeID)
	if err != nil {
		return nil, err
	}

	// Get slice
	slice, found := s.slices.Get(sliceKey)
	if !found {
		return nil, errors.Errorf("slice not found %q", sliceKey.String())
	}

	// Open writer
	w, err := vol.OpenWriter(req.SourceNodeId, sliceKey, slice.LocalStorage)
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

func sliceKeyFrom(k *pb.SliceKey) storage.SliceKey {
	return storage.SliceKey{
		FileVolumeKey: storage.FileVolumeKey{
			FileKey: storage.FileKey{
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
				FileID: storage.FileID{
					OpenedAt: utctime.From(k.FileId.AsTime()),
				},
			},
			VolumeID: volume.ID(k.VolumeId),
		},
		SliceID: storage.SliceID{
			OpenedAt: utctime.From(k.SliceId.AsTime()),
		},
	}
}
