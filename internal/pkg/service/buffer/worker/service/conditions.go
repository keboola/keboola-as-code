package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinimalCredentialsExpiration which triggers the import.
	MinimalCredentialsExpiration = time.Hour
)

type checker struct {
	*Service
	logger log.Logger

	checkLock        *sync.Mutex
	uploadConditions model.Conditions
	importConditions cachedConditions
	openedSlices     cachedSlices
}

type cachedConditions map[key.ExportKey]model.Conditions

type cachedSlices map[key.SliceKey]cachedSlice

type cachedSlice struct {
	// expiration of the slice upload credentials.
	expiration time.Time
}

func startChecker(s *Service) <-chan error {
	c := &checker{
		Service:          s,
		logger:           s.logger.AddPrefix("[conditions]"),
		checkLock:        &sync.Mutex{},
		uploadConditions: model.Conditions(s.config.UploadConditions),
		importConditions: make(cachedConditions),
		openedSlices:     make(cachedSlices),
	}

	// Start watchers and ticker
	initDone := make(chan error, 1)
	c.Service.wg.Add(1)
	go func() {
		defer c.Service.wg.Done()
		defer close(initDone)
		startTime := c.clock.Now()

		if err := <-c.watchImportConditions(c.Service.ctx, c.Service.wg); err != nil {
			initDone <- err
			return
		}

		if err := <-c.watchOpenedSlices(c.Service.ctx, c.Service.wg); err != nil {
			initDone <- err
			return
		}

		c.startTicker(c.Service.ctx, c.Service.wg)
		c.logger.Infof(`initialized | %s`, c.clock.Since(startTime))
	}()
	return initDone
}

// checkConditions periodically check file import and slice upload conditions.
func (s *Service) checkConditions() <-chan error {
	return startChecker(s)
}

func (c *checker) check(ctx context.Context) {
	c.checkLock.Lock()
	defer c.checkLock.Unlock()

	now := c.clock.Now()
	for sliceKey, slice := range c.openedSlices {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Import file after upload of the last slice
		if met, reason, err := c.shouldImport(ctx, now, sliceKey, slice.expiration); err != nil {
			c.logger.Error(err)
			continue
		} else if met {
			if err := c.swapFile(sliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		} else if reason != "" {
			c.logger.Debugf(`skipped import of the file "%s": %s`, sliceKey.FileKey, reason)
		}

		// Upload slice
		if met, reason, err := c.shouldUpload(ctx, now, sliceKey); err != nil {
			c.logger.Error(err)
			continue
		} else if met {
			if err := c.swapSlice(sliceKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		} else if reason != "" {
			c.logger.Debugf(`skipped upload of the slice "%s": %s`, sliceKey, reason)
		}
	}
	c.logger.Debugf(`checked "%d" opened slices | %s`, len(c.openedSlices), c.clock.Since(now))
}

func (c *checker) shouldImport(ctx context.Context, now time.Time, sliceKey key.SliceKey, uploadCredExp time.Time) (ok bool, reason string, err error) {
	// Check minimal interval
	if interval := now.Sub(sliceKey.FileKey.OpenedAt()); interval < c.config.MinimalImportInterval {
		reason = fmt.Sprintf(`interval "%s" is less than the MinimalImportInterval "%s"`, interval, c.config.MinimalImportInterval)
		return false, reason, nil
	}

	// Check credentials expiration
	if uploadCredExp.Sub(now) <= MinimalCredentialsExpiration {
		reason = fmt.Sprintf("upload credentials will expire soon, at %s", uploadCredExp.UTC().String())
		return true, reason, nil
	}

	// Get import conditions
	cdn, found := c.importConditions[sliceKey.ExportKey]
	if !found {
		reason = "import conditions not found"
		return false, reason, nil
	}

	// Get file stats
	fileStats, err := c.cachedStats.FileStats(ctx, sliceKey.FileKey)
	if err != nil {
		return false, "", err
	}

	// Evaluate import conditions
	ok, reason = cdn.Evaluate(now, sliceKey.FileKey.OpenedAt(), fileStats.Total)
	return ok, reason, nil
}

func (c *checker) shouldUpload(ctx context.Context, now time.Time, sliceKey key.SliceKey) (ok bool, reason string, err error) {
	// Check minimal interval
	if interval := now.Sub(sliceKey.OpenedAt()); interval < c.config.MinimalUploadInterval {
		reason = fmt.Sprintf(`interval "%s" is less than the MinimalUploadInterval "%s"`, interval, c.config.MinimalUploadInterval)
		return false, reason, nil
	}

	// Get slice stats
	sliceStats, err := c.cachedStats.SliceStats(ctx, sliceKey)
	if err != nil {
		return false, "", err
	}

	// Evaluate upload conditions
	ok, reason = c.uploadConditions.Evaluate(now, sliceKey.OpenedAt(), sliceStats.Total)
	return ok, reason, nil
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
				if !c.Service.dist.MustCheckIsOwner(export.ReceiverKey.String()) {
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
				if !c.Service.dist.MustCheckIsOwner(slice.ReceiverKey.String()) {
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
