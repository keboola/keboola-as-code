// Package conditions provides the Checker to periodically check file import and slice upload conditions.
package conditions

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	CheckInterval                = 30 * time.Second
	MinimalCredentialsExpiration = time.Hour
)

type Checker struct {
	clock            clock.Clock
	logger           log.Logger
	schema           *schema.Schema
	etcdClient       *etcd.Client
	httpClient       client.Client
	storageAPIHost   string
	store            *store.Store
	tasks            *task.Node
	dist             *distribution.Node
	statsCache       *statistics.CacheNode
	uploadConditions model.Conditions
}

type conditionsMap map[key.ExportKey]model.Conditions

type slicesMap map[key.SliceKey]sliceInfo

// expiration of the slice upload credentials.
type sliceInfo struct {
	expiration *time.Time
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
	HTTPClient() client.Client
	StorageAPIHost() string
	Store() *store.Store
	TaskWorkerNode() *task.Node
	DistributionWorkerNode() *distribution.Node
	StatsCacheNode() *statistics.CacheNode
}

func NewChecker(d dependencies) (*Checker, error) {
	v := &Checker{
		clock:            d.Clock(),
		logger:           d.Logger().AddPrefix("[conditions]"),
		schema:           d.Schema(),
		etcdClient:       d.EtcdClient(),
		httpClient:       d.HTTPClient(),
		storageAPIHost:   d.StorageAPIHost(),
		store:            d.Store(),
		tasks:            d.TaskWorkerNode(),
		dist:             d.DistributionWorkerNode(),
		statsCache:       d.StatsCacheNode(),
		uploadConditions: model.UploadConditions(),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		v.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		v.logger.Info("shutdown done")
	})

	// Stop on initialization error
	startTime := time.Now()
	if err := <-v.start(ctx, wg); err != nil {
		return nil, err
	}

	v.logger.Infof(`initialized | %s`, time.Since(startTime))
	return v, nil
}

func (v *Checker) check(ctx context.Context, lock *sync.RWMutex, importConditionsMap conditionsMap, openedSlicesMap slicesMap) {
	lock.RLock()
	defer lock.RUnlock()

	now := v.clock.Now()
	for sliceKey, slice := range openedSlicesMap {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check credentials expiration
		if slice.expiration != nil {
			exp := *slice.expiration
			if exp.Sub(now) <= MinimalCredentialsExpiration {
				if err := v.closeFile(ctx, sliceKey.FileKey, fmt.Sprintf("upload credentials will expire soon, at %s", exp.UTC().String())); err != nil {
					v.logger.Error(err)
				}
				continue
			}
		}

		// Get import conditions
		cdn, found := importConditionsMap[sliceKey.ExportKey]
		if !found {
			continue
		}

		// Check import conditions
		if met, reason := cdn.Evaluate(now, sliceKey.FileKey.OpenedAt(), v.statsCache.FileStats(sliceKey.FileKey).Total); met {
			if err := v.closeFile(ctx, sliceKey.FileKey, reason); err != nil {
				v.logger.Error(err)
			}
			continue
		}

		// Check upload conditions
		if met, reason := v.uploadConditions.Evaluate(now, sliceKey.OpenedAt(), v.statsCache.SliceStats(sliceKey).Total); met {
			if err := v.closeSlice(ctx, sliceKey, reason); err != nil {
				v.logger.Error(err)
			}
			continue
		}
	}
	v.logger.Infof(`checked "%d" opened slices | %s`, len(openedSlicesMap), v.clock.Since(now))
}

func (v *Checker) closeFile(ctx context.Context, fileKey key.FileKey, reason string) (err error) {
	lock := "file.closing/" + fileKey.FileID.String()
	_, err = v.tasks.StartTask(ctx, fileKey.ExportKey, "file.closing", lock, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		v.logger.Infof(`closing file "%s": %s`, fileKey, reason)
		rb := rollback.New(v.logger)
		defer rb.InvokeIfErr(ctx, &err)

		// Get export
		export, err := v.store.GetExport(ctx, fileKey.ExportKey)
		if err != nil {
			return "", errors.Errorf(`cannot close file "%s": %w`, fileKey.String(), err)
		}

		oldFile := export.OpenedFile
		if oldFile.FileKey != fileKey {
			return "", errors.Errorf(`cannot close file "%s": unexpected export opened file "%s"`, fileKey.String(), oldFile.FileKey)
		}

		oldSlice := export.OpenedSlice
		if oldSlice.FileKey != fileKey {
			return "", errors.Errorf(`cannot close file "%s": unexpected export opened slice "%s"`, fileKey.String(), oldFile.FileKey)
		}

		apiClient := storageapi.ClientWithHostAndToken(v.httpClient, v.storageAPIHost, export.Token.Token)
		files := file.NewManager(v.clock, apiClient, nil)

		if err := files.CreateFileForExport(ctx, rb, &export); err != nil {
			return "", errors.Errorf(`cannot close file "%s": cannot create new file: %w`, fileKey.String(), err)
		}

		if err := v.store.SwapFile(ctx, &oldFile, &oldSlice, export.OpenedFile, export.OpenedSlice); err != nil {
			return "", errors.Errorf(`cannot close file "%s": cannot swap old and new file: %w`, fileKey.String(), err)
		}

		return "file switched to the closing state", nil
	})
	return err
}

func (v *Checker) closeSlice(ctx context.Context, sliceKey key.SliceKey, reason string) (err error) {
	lock := "slice.closing/" + sliceKey.SliceID.String()
	_, err = v.tasks.StartTask(ctx, sliceKey.ExportKey, "slice.closing", lock, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		v.logger.Infof(`closing slice "%s": %s`, sliceKey, reason)
		rb := rollback.New(v.logger)
		defer rb.InvokeIfErr(ctx, &err)

		// Get export
		export, err := v.store.GetExport(ctx, sliceKey.ExportKey)
		if err != nil {
			return "", errors.Errorf(`cannot close slice "%s": %w`, sliceKey.String(), err)
		}

		oldSlice := export.OpenedSlice
		if oldSlice.SliceKey != sliceKey {
			return "", errors.Errorf(`cannot close slice "%s": unexpected export opened slice "%s"`, sliceKey.String(), oldSlice.FileKey)
		}

		export.OpenedSlice = model.NewSlice(oldSlice.FileKey, v.clock.Now(), oldSlice.Mapping, oldSlice.Number+1, oldSlice.StorageResource)
		if newSlice, err := v.store.SwapSlice(ctx, &oldSlice); err == nil {
			export.OpenedSlice = newSlice
		} else {
			return "", errors.Errorf(`cannot close slice "%s": cannot swap old and new slice: %w`, sliceKey.String(), err)
		}

		return "slice switched to the closing state", nil
	})
	return err
}

func (v *Checker) start(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	work := func(ctx context.Context, assigner *distribution.Assigner) (initDone <-chan error) {
		lock := &sync.RWMutex{}
		importConditions := make(conditionsMap)
		openedSlices := make(slicesMap)

		// Watch import conditions
		init1 := v.schema.Configs().Exports().
			GetAllAndWatch(ctx, v.etcdClient, etcd.WithPrevKV()).
			SetupConsumer(v.logger).
			WithForEach(func(events []etcdop.WatchEventT[model.ExportBase], header *etcdop.Header, restart bool) {
				lock.Lock()
				defer lock.Unlock()
				for _, event := range events {
					export := event.Value
					if !assigner.MustCheckIsOwner(export.ReceiverKey.String()) {
						// Another worker node handles the resource.
						continue
					}

					switch event.Type {
					case etcdop.CreateEvent, etcdop.UpdateEvent:
						importConditions[export.ExportKey] = export.ImportConditions
					case etcdop.DeleteEvent:
						delete(importConditions, export.ExportKey)
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}
			}).
			StartConsumer(wg)

		// Watch opened slices
		init2 := v.schema.Slices().Opened().
			GetAllAndWatch(ctx, v.etcdClient, etcd.WithPrevKV()).
			SetupConsumer(v.logger).
			WithForEach(func(events []etcdop.WatchEventT[model.Slice], header *etcdop.Header, restart bool) {
				lock.Lock()
				defer lock.Unlock()
				for _, event := range events {
					slice := event.Value
					if !assigner.MustCheckIsOwner(slice.ReceiverKey.String()) {
						// Another worker node handles the resource.
						continue
					}

					switch event.Type {
					case etcdop.CreateEvent, etcdop.UpdateEvent:
						openedSlices[slice.SliceKey] = sliceInfo{
							expiration: getCredentialsExpiration(slice),
						}
					case etcdop.DeleteEvent:
						delete(openedSlices, slice.SliceKey)
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}
			}).
			StartConsumer(wg)

		// Check conditions periodically
		wg.Add(1)
		go func() {
			defer wg.Done()

			ticker := v.clock.Ticker(CheckInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					v.check(ctx, lock, importConditions, openedSlices)
				}
			}
		}()

		// Wait for initialization of both watchers
		initDoneCh := make(chan error, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs := errors.NewMultiError()
			if err := <-init1; err != nil {
				errs.Append(err)
			}
			if err := <-init2; err != nil {
				errs.Append(err)
			}
			if err := errs.ErrorOrNil(); err != nil {
				initDoneCh <- err
			}
			close(initDoneCh)
		}()
		return initDoneCh
	}
	return v.dist.StartWork(ctx, wg, v.logger, work)
}

func getCredentialsExpiration(slice model.Slice) *time.Time {
	switch {
	case slice.StorageResource.S3UploadParams != nil:
		return &slice.StorageResource.S3UploadParams.Credentials.Expiration.Time
	case slice.StorageResource.ABSUploadParams != nil:
		return &slice.StorageResource.ABSUploadParams.Credentials.Expiration.Time
	case slice.StorageResource.GCSUploadParams != nil:
		// return slice.StorageResource.GCSUploadParams.ExpiresIn ??? seconds not time
		return nil
	default:
		panic(errors.New(`no upload parameters found`))
	}
}
