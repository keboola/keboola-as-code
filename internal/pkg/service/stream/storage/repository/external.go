package repository

//
//import (
//	"context"
//	"fmt"
//	"sync"
//	"time"
//
//	"github.com/c2h5oh/datasize"
//	"github.com/keboola/go-client/pkg/keboola"
//	etcd "go.etcd.io/etcd/client/v3"
//	"golang.org/x/sync/errgroup"
//
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
//	statsRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
//)
//
//// recordsForSliceDiskSizeCalc defines the number of last slice statistics that are taken into account
//// when calculating the amount of disk space that needs to be pre-allocated for a new slice.
//const recordsForSliceDiskSizeCalc = 10
//
//// FileResource is an auxiliary struct that connect the stream storage.FileKey with the Keboola file resource.
//type FileResource struct {
//	model.FileKey
//	Credentials *keboola.FileUploadCredentials
//}
//
//// FileResourcesProvider is a function that in parallel creates file resources for the provided sinks.
//// The function can be called multiple times within op.AtomicOp retries,
//// so the result map should be cached.
//type FileResourcesProvider func(ctx context.Context, now time.Time, sinkKeys []key.SinkKey) (map[key.SinkKey]*FileResource, error)
//
//// UsedDiskSpaceProvider provides maximum size of previous uploaded/imported slices.
//// The result is used to pre-allocate disk space for a new slice.
//type UsedDiskSpaceProvider func(ctx context.Context, sinkKeys []key.SinkKey) (map[key.SinkKey]datasize.ByteSize, error)
//
//// external provide bridge to other parts of the system that are not part of the repository, but are needed for repository operations.
//type external struct {
//	client    *etcd.Client
//	publicAPI *keboola.PublicAPI
//	config    level.Config
//	stats     *statsRepo.Repository
//	storage   *Repository
//}
//
//func newExternal(cfg level.Config, d dependencies, repo *Repository) *external {
//	return &external{
//		client:    d.EtcdClient(),
//		publicAPI: d.KeboolaPublicAPI(),
//		config:    cfg,
//		stats:     d.StatisticsRepository(),
//		storage:   repo,
//	}
//}
//
//func (e *external) NewFileResourcesProvider(rb rollback.Builder) FileResourcesProvider {
//	result := make(map[key.SinkKey]*FileResource)
//	rb = rb.AddParallel()
//	lock := &sync.Mutex{}
//	return func(ctx context.Context, now time.Time, sinkKeys []key.SinkKey) (map[key.SinkKey]*FileResource, error) {
//		// Get sinks tokens
//		tokens := make(map[key.SinkKey]string)
//		txn := op.Txn(e.client)
//		for _, sinkKey := range sinkKeys {
//			// Get token only once, the provider can be reused within op.AtomicOp retries.
//			if _, ok := tokens[sinkKey]; !ok {
//				txn.Then(e.storage.Token().Get(sinkKey).WithOnResult(func(result model.Token) {
//					tokens[sinkKey] = result.Token.Token
//				}))
//			}
//		}
//		if err := txn.Do(ctx).Err(); err != nil {
//			return nil, err
//		}
//
//		// Create file resources
//		grp, ctx := errgroup.WithContext(ctx)
//		grp.SetLimit(e.config.Staging.ParallelFileCreateLimit)
//		for _, sinkKey := range sinkKeys {
//			sinkKey := sinkKey
//
//			// Create file resource only once, the provider can be reused within op.AtomicOp retries.
//			lock.Lock()
//			_, ok := result[sinkKey]
//			lock.Unlock()
//			if ok {
//				continue
//			}
//
//			// Authorize API
//			api := e.publicAPI.WithToken(tokens[sinkKey])
//
//			// Create file resource in parallel
//			grp.Go(func() error {
//				// Generate file key
//				fileKey := model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: utctime.From(now)}}
//
//				// Generate file resource name
//				fileName := fmt.Sprintf(`stream_%s_%s_%s`, fileKey.SourceID, fileKey.SinkID, fileKey.FileID)
//
//				// Create file resource in the staging storage
//				credentials, err := api.CreateFileResourceRequest(
//					sinkKey.BranchID,
//					fileName,
//					keboola.WithIsSliced(true),
//					keboola.WithTags(
//						fmt.Sprintf("stream.sourceID=%s", fileKey.SourceID),
//						fmt.Sprintf("stream.sinkID=%s", fileKey.SinkID),
//					),
//				).Send(ctx)
//				if err != nil {
//					return err
//				}
//
//				// Register rollback, if some other operation fails
//				rb.Add(func(ctx context.Context) error {
//					return api.DeleteFileRequest(credentials.FileKey).SendOrErr(ctx)
//				})
//
//				lock.Lock()
//				result[sinkKey] = &FileResource{FileKey: fileKey, Credentials: credentials}
//				lock.Unlock()
//				return nil
//			})
//		}
//
//		// Wait for goroutines
//		if err := grp.Wait(); err != nil {
//			return nil, errors.PrefixError(err, "cannot create file resource")
//		}
//
//		return result, nil
//	}
//}
