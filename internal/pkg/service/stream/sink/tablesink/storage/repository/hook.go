package repository

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics"
	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume/assignment"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// recordsForSliceDiskSizeCalc defines the number of last slice statistics that are taken into account
// when calculating the amount of disk space that needs to be pre-allocated for a new slice.
const recordsForSliceDiskSizeCalc = 10

// FileResource is an auxiliary struct that connect the stream storage.FileKey with the Keboola file resource.
type FileResource struct {
	storage.FileKey
	Credentials *keboola.FileUploadCredentials
}

// FileResourcesProvider is a function that in parallel creates file resources for the provided sinks.
// The function can be called multiple times within op.AtomicOp retries,
// so the result map should be cached.
type FileResourcesProvider func(ctx context.Context, now time.Time, sinkKeys []key.SinkKey) (map[key.SinkKey]*FileResource, error)

// UsedDiskSpaceProvider provides maximum size of previous uploaded/imported slices.
// The result is used to pre-allocate disk space for a new slice.
type UsedDiskSpaceProvider func(ctx context.Context, sinkKeys []key.SinkKey) (map[key.SinkKey]datasize.ByteSize, error)

// hook provide bridge to other parts of the system that are not part of the repository, but are needed for repository operations.
type hook struct {
	client    *etcd.Client
	publicAPI *keboola.PublicAPI
	config    config.Config
	stats     *statsRepo.Repository
	storage   *Repository
}

func newHook(d dependencies, repo *Repository) *hook {
	return &hook{
		client:    d.EtcdClient(),
		publicAPI: d.KeboolaPublicAPI(),
		config:    d.Config(),
		stats:     d.StatisticsRepository(),
		storage:   repo,
	}
}

// AssignVolumes assigns volumes to a new file.
func (h *hook) AssignVolumes(_ context.Context, allVolumes []volume.Metadata, cfg assignment.Config, fileOpenedAt time.Time) assignment.Assignment {
	return assignment.VolumesFor(allVolumes, cfg, fileOpenedAt.UnixNano())
}

func (h *hook) NewFileResourcesProvider(rb rollback.Builder) FileResourcesProvider {
	result := make(map[key.SinkKey]*FileResource)
	rb = rb.AddParallel()
	lock := &sync.Mutex{}
	return func(ctx context.Context, now time.Time, sinkKeys []key.SinkKey) (map[key.SinkKey]*FileResource, error) {
		// Get sinks tokens
		tokens := make(map[key.SinkKey]string)
		txn := op.Txn(h.client)
		for _, sinkKey := range sinkKeys {
			// Get token only once, the provider can be reused within op.AtomicOp retries.
			if _, ok := tokens[sinkKey]; !ok {
				txn.Then(h.storage.Token().Get(sinkKey).WithOnResult(func(result storage.Token) {
					tokens[sinkKey] = result.Token.Token
				}))
			}
		}
		if err := txn.Do(ctx).Err(); err != nil {
			return nil, err
		}

		// Create file resources
		grp, ctx := errgroup.WithContext(ctx)
		grp.SetLimit(h.config.Sink.Table.Storage.Staging.ParallelFileCreateLimit)
		for _, sinkKey := range sinkKeys {
			sinkKey := sinkKey

			// Create file resource only once, the provider can be reused within op.AtomicOp retries.
			lock.Lock()
			_, ok := result[sinkKey]
			lock.Unlock()
			if ok {
				continue
			}

			// Authorize API
			api := h.publicAPI.WithToken(tokens[sinkKey])

			// Create file resource in parallel
			grp.Go(func() error {
				// Generate file key
				fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(now)}}

				// Generate file resource name
				fileName := fmt.Sprintf(`stream_%s_%s_%s`, fileKey.SourceID, fileKey.SinkID, fileKey.FileID)

				// Create file resource in the staging storage
				credentials, err := api.CreateFileResourceRequest(
					sinkKey.BranchID,
					fileName,
					keboola.WithIsSliced(true),
					keboola.WithTags(
						fmt.Sprintf("stream.sourceID=%s", fileKey.SourceID),
						fmt.Sprintf("stream.sinkID=%s", fileKey.SinkID),
					),
				).Send(ctx)
				if err != nil {
					return err
				}

				// Register rollback, if some other operation fails
				rb.Add(func(ctx context.Context) error {
					return api.DeleteFileRequest(credentials.FileKey).SendOrErr(ctx)
				})

				lock.Lock()
				result[sinkKey] = &FileResource{FileKey: fileKey, Credentials: credentials}
				lock.Unlock()
				return nil
			})
		}

		// Wait for goroutines
		if err := grp.Wait(); err != nil {
			return nil, errors.PrefixError(err, "cannot create file resource")
		}

		return result, nil
	}
}

func (h *hook) NewUsedDiskSpaceProvider() UsedDiskSpaceProvider {
	result := make(map[key.SinkKey]datasize.ByteSize)
	return func(ctx context.Context, sinkKeys []key.SinkKey) (map[key.SinkKey]datasize.ByteSize, error) {
		txn := op.Txn(h.client)
		for _, sinkKey := range sinkKeys {
			// Load statistics only once, the provider can be reused within op.AtomicOp retries.
			if _, ok := result[sinkKey]; !ok {
				txn.Merge(h.stats.
					MaxUsedDiskSizeBySliceIn(sinkKey, recordsForSliceDiskSizeCalc).
					OnResult(func(r *op.TxnResult[datasize.ByteSize]) {
						result[sinkKey] = r.Result()
					}),
				)
			}
		}

		// Get all in one transaction
		if !txn.Empty() {
			if err := txn.Do(ctx).Err(); err != nil {
				return nil, err
			}
		}

		return result, nil
	}
}

func (h *hook) DecorateFileStateTransition(atomicOp *op.AtomicOp[storage.File], fileKey storage.FileKey, from, to storage.FileState) *op.AtomicOp[storage.File] {
	// Move statistics to the target storage level, if needed
	fromLevel := from.Level()
	toLevel := to.Level()
	if fromLevel != toLevel {
		atomicOp.AddFrom(h.stats.MoveAll(fileKey, fromLevel, toLevel, func(value *statistics.Value) {
			// There is actually no additional compression, when uploading slice to the staging storage
			if toLevel == storage.LevelStaging {
				value.StagingSize = value.CompressedSize
			}
		}))
	}
	return atomicOp
}

func (h *hook) DecorateSliceStateTransition(atomicOp *op.AtomicOp[storage.Slice], sliceKey storage.SliceKey, from, to storage.SliceState) *op.AtomicOp[storage.Slice] {
	// Move statistics to the target storage level, if needed
	fromLevel := from.Level()
	toLevel := to.Level()
	if fromLevel != toLevel {
		atomicOp.AddFrom(h.stats.Move(sliceKey, fromLevel, toLevel, func(value *statistics.Value) {
			// There is actually no additional compression, when uploading slice to the staging storage
			if toLevel == storage.LevelStaging {
				value.StagingSize = value.CompressedSize
			}
		}))
	}
	return atomicOp
}

func (h *hook) DecorateFileDelete(atomicOp *op.AtomicOp[op.NoResult], fileKey storage.FileKey) *op.AtomicOp[op.NoResult] {
	// Delete/rollup statistics
	atomicOp.AddFrom(h.stats.Delete(fileKey))
	return atomicOp
}

func (h *hook) DecorateSliceDelete(atomicOp *op.AtomicOp[op.NoResult], sliceKey storage.SliceKey) *op.AtomicOp[op.NoResult] {
	// Delete/rollup statistics
	atomicOp.AddFrom(h.stats.Delete(sliceKey))
	return atomicOp
}
