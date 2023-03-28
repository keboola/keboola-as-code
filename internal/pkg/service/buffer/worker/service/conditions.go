package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinimalCredentialsExpiration which triggers the import.
	MinimalCredentialsExpiration = time.Hour

	fileSwapTaskType  = "file.swap"
	sliceSwapTaskType = "slice.swap"
)

type checker struct {
	*Service
	logger   log.Logger
	assigner *distribution.Assigner

	lock             *sync.RWMutex
	importConditions cachedConditions
	openedSlices     cachedSlices
}

type cachedConditions map[key.ExportKey]model.Conditions

type cachedSlices map[key.SliceKey]cachedSlice

type cachedSlice struct {
	// expiration of the slice upload credentials.
	expiration time.Time
}

func startChecker(ctx context.Context, wg *sync.WaitGroup, s *Service, assigner *distribution.Assigner) <-chan error {
	c := &checker{
		Service:          s,
		logger:           s.logger.AddPrefix("[conditions]"),
		assigner:         assigner,
		lock:             &sync.RWMutex{},
		importConditions: make(cachedConditions),
		openedSlices:     make(cachedSlices),
	}

	// Start watchers and ticker
	initDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(initDone)
		startTime := c.clock.Now()

		if err := <-c.watchImportConditions(ctx, wg); err != nil {
			initDone <- err
			return
		}

		if err := <-c.watchOpenedSlices(ctx, wg); err != nil {
			initDone <- err
			return
		}

		c.startTicker(ctx, wg)
		c.logger.Infof(`initialized | %s`, c.clock.Since(startTime))
	}()
	return initDone
}

// checkConditions periodically check file import and slice upload conditions.
func (s *Service) checkConditions(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	// Checker is re-created on each distribution change
	return s.dist.StartWork(ctx, wg, s.logger, func(ctx context.Context, assigner *distribution.Assigner) (initDone <-chan error) {
		return startChecker(ctx, wg, s, assigner)
	})
}

func (c *checker) check(ctx context.Context) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	now := c.clock.Now()
	for sliceKey, slice := range c.openedSlices {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check credentials expiration
		if slice.expiration.Sub(now) <= MinimalCredentialsExpiration {
			reason := fmt.Sprintf("upload credentials will expire soon, at %s", slice.expiration.UTC().String())
			if err := c.swapFile(ctx, sliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		}

		// Get import conditions
		cdn, found := c.importConditions[sliceKey.ExportKey]
		if !found {
			continue
		}

		// Check import conditions
		if met, reason := cdn.Evaluate(now, sliceKey.FileKey.OpenedAt(), c.stats.FileStats(sliceKey.FileKey).Total); met {
			if err := c.swapFile(ctx, sliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		}

		// Check upload conditions
		if met, reason := c.config.UploadConditions.Evaluate(now, sliceKey.OpenedAt(), c.stats.SliceStats(sliceKey).Total); met {
			if err := c.swapSlice(ctx, sliceKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		}
	}
	c.logger.Infof(`checked "%d" opened slices | %s`, len(c.openedSlices), c.clock.Since(now))
}

func (c *checker) watchImportConditions(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	return c.schema.Configs().Exports().
		GetAllAndWatch(ctx, c.etcdClient, etcd.WithPrevKV()).
		SetupConsumer(c.logger).
		WithForEach(func(events []etcdop.WatchEventT[model.ExportBase], header *etcdop.Header, restart bool) {
			c.lock.Lock()
			defer c.lock.Unlock()
			for _, event := range events {
				export := event.Value
				if !c.assigner.MustCheckIsOwner(export.ReceiverKey.String()) {
					// Another worker node handles the resource.
					delete(c.importConditions, export.ExportKey)
					continue
				}

				switch event.Type {
				case etcdop.CreateEvent, etcdop.UpdateEvent:
					c.importConditions[export.ExportKey] = export.ImportConditions
				case etcdop.DeleteEvent:
					delete(c.importConditions, export.ExportKey)
				default:
					panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
				}
			}
		}).
		StartConsumer(wg)
}

func (c *checker) watchOpenedSlices(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	// Watch opened slices
	return c.schema.Slices().Writing().
		GetAllAndWatch(ctx, c.etcdClient, etcd.WithPrevKV()).
		SetupConsumer(c.logger).
		WithForEach(func(events []etcdop.WatchEventT[model.Slice], header *etcdop.Header, restart bool) {
			c.lock.Lock()
			defer c.lock.Unlock()
			for _, event := range events {
				slice := event.Value
				if !c.assigner.MustCheckIsOwner(slice.ReceiverKey.String()) {
					// Another worker node handles the resource.
					delete(c.openedSlices, slice.SliceKey)
					continue
				}

				switch event.Type {
				case etcdop.CreateEvent, etcdop.UpdateEvent:
					c.openedSlices[slice.SliceKey] = cachedSlice{
						expiration: getCredentialsExpiration(slice),
					}
				case etcdop.DeleteEvent:
					delete(c.openedSlices, slice.SliceKey)
				default:
					panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
				}
			}
		}).
		StartConsumer(wg)
}

// startTicker to check conditions periodically.
func (c *checker) startTicker(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := c.clock.Ticker(c.config.CheckConditionsInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.check(ctx)
			}
		}
	}()
}

func (c *checker) swapFile(ctx context.Context, fileKey key.FileKey, reason string) (err error) {
	taskKey := key.TaskKey{
		ProjectID: fileKey.ProjectID,
		TaskID: key.TaskID(strings.Join([]string{
			fileKey.ReceiverID.String(),
			fileKey.ExportID.String(),
			fileKey.FileID.String(),
			fileSwapTaskType,
		}, "/")),
	}

	_, err = c.tasks.StartTask(ctx, taskKey, fileSwapTaskType, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		c.logger.Infof(`closing file "%s": %s`, fileKey, reason)
		rb := rollback.New(c.logger)
		defer rb.InvokeIfErr(ctx, &err)

		// Get export
		export, err := c.store.GetExport(ctx, fileKey.ExportKey)
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

		api, err := keboola.NewAPI(ctx, c.storageAPIHost, keboola.WithClient(&c.httpClient), keboola.WithToken(export.Token.Token))
		if err != nil {
			return "", err
		}
		files := file.NewManager(c.clock, api, nil)

		if err := files.CreateFileForExport(ctx, rb, &export); err != nil {
			return "", errors.Errorf(`cannot close file "%s": cannot create new file: %w`, fileKey.String(), err)
		}

		if err := c.store.SwapFile(ctx, &oldFile, &oldSlice, export.OpenedFile, export.OpenedSlice); err != nil {
			return "", errors.Errorf(`cannot close file "%s": cannot swap old and new file: %w`, fileKey.String(), err)
		}

		return "new file created, the old is closing", nil
	})
	return err
}

func (c *checker) swapSlice(ctx context.Context, sliceKey key.SliceKey, reason string) (err error) {
	taskKey := key.TaskKey{
		ProjectID: sliceKey.ProjectID,
		TaskID: key.TaskID(strings.Join([]string{
			sliceKey.ReceiverID.String(),
			sliceKey.ExportID.String(),
			sliceKey.FileID.String(),
			sliceKey.SliceID.String(),
			sliceSwapTaskType,
		}, "/")),
	}

	_, err = c.tasks.StartTask(ctx, taskKey, sliceSwapTaskType, func(ctx context.Context, logger log.Logger) (task.Result, error) {
		c.logger.Infof(`closing slice "%s": %s`, sliceKey, reason)
		rb := rollback.New(c.logger)
		defer rb.InvokeIfErr(ctx, &err)

		// Get export
		export, err := c.store.GetExport(ctx, sliceKey.ExportKey)
		if err != nil {
			return "", errors.Errorf(`cannot close slice "%s": %w`, sliceKey.String(), err)
		}

		oldSlice := export.OpenedSlice
		if oldSlice.SliceKey != sliceKey {
			return "", errors.Errorf(`cannot close slice "%s": unexpected export opened slice "%s"`, sliceKey.String(), oldSlice.FileKey)
		}

		export.OpenedSlice = model.NewSlice(oldSlice.FileKey, c.clock.Now(), oldSlice.Mapping, oldSlice.Number+1, oldSlice.StorageResource)
		if newSlice, err := c.store.SwapSlice(ctx, &oldSlice); err == nil {
			export.OpenedSlice = newSlice
		} else {
			return "", errors.Errorf(`cannot close slice "%s": cannot swap old and new slice: %w`, sliceKey.String(), err)
		}

		return "new slice created, the old is closing", nil
	})
	return err
}

func getCredentialsExpiration(slice model.Slice) time.Time {
	switch {
	case slice.StorageResource.S3UploadParams != nil:
		return slice.StorageResource.S3UploadParams.Credentials.Expiration.Time
	case slice.StorageResource.ABSUploadParams != nil:
		return slice.StorageResource.ABSUploadParams.Credentials.Expiration.Time
	case slice.StorageResource.GCSUploadParams != nil:
		return slice.OpenedAt().Add(time.Second * time.Duration(slice.StorageResource.GCSUploadParams.ExpiresIn))
	default:
		panic(errors.New(`no upload parameters found`))
	}
}
