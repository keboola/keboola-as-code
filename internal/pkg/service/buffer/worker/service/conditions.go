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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinimalCredentialsExpiration which triggers the import.
	MinimalCredentialsExpiration = time.Hour
)

type checker struct {
	*Service
	logger           log.Logger
	uploadConditions model.Conditions

	checkLock    *sync.Mutex
	exports      *etcdop.Mirror[model.ExportBase, cachedExport]
	activeSlices *etcdop.Mirror[model.Slice, cachedSlice]
}

type cachedExport struct {
	key.ExportKey
	ImportConditions model.Conditions
}

type cachedSlice struct {
	key.SliceKey
	CredExpiration time.Time
}

func startChecker(s *Service) <-chan error {
	c := &checker{
		Service:          s,
		logger:           s.logger.AddPrefix("[conditions]"),
		uploadConditions: model.Conditions(s.config.UploadConditions),
		checkLock:        &sync.Mutex{},
	}

	// Start watchers and ticker
	initDone := make(chan error, 1)
	c.Service.wg.Add(1)
	go func() {
		defer c.Service.wg.Done()
		defer close(initDone)
		startTime := c.clock.Now()

		if err := <-c.watchExports(c.Service.ctx, c.Service.wg); err != nil {
			initDone <- err
			return
		}

		if err := <-c.watchActiveSlices(c.Service.ctx, c.Service.wg); err != nil {
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
	checked := 0
	c.activeSlices.WalkAll(func(_ string, slice cachedSlice) (stop bool) {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		// Try swap the file, it includes also swap of the slice
		importOk, reason, err := c.shouldImport(ctx, now, slice.SliceKey, slice.CredExpiration)
		if err != nil {
			c.logger.Error(err)
		} else if importOk {
			if err := c.swapFile(slice.SliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
		} else if reason != "" {
			c.logger.Debugf(`skipped import of the file "%s": %s`, slice.SliceKey.FileKey, reason)
		}

		// Try swap e slice, if it didn't already happen during import
		if !importOk {
			uploadOk, reason, err := c.shouldUpload(ctx, now, slice.SliceKey)
			if err != nil {
				c.logger.Error(err)
			} else if uploadOk {
				if err := c.swapSlice(slice.SliceKey, reason); err != nil {
					c.logger.Error(err)
				}
			} else if reason != "" {
				c.logger.Debugf(`skipped upload of the slice "%s": %s`, slice.SliceKey, reason)
			}
		}

		checked++
		return false
	})

	c.logger.Debugf(`checked "%d" opened slices | %s`, checked, c.clock.Since(now))
}

func (c *checker) shouldImport(ctx context.Context, now time.Time, sliceKey key.SliceKey, uploadCredExp time.Time) (ok bool, reason string, err error) {
	// Check minimal interval
	if interval := now.Sub(sliceKey.FileKey.OpenedAt()); interval < c.config.MinimalImportInterval {
		reason = fmt.Sprintf(`interval "%s" is less than the MinimalImportInterval "%s"`, interval, c.config.MinimalImportInterval)
		return false, reason, nil
	}

	// Check credentials CredExpiration
	if uploadCredExp.Sub(now) <= MinimalCredentialsExpiration {
		reason = fmt.Sprintf("upload credentials will expire soon, at %s", uploadCredExp.UTC().String())
		return true, reason, nil
	}

	// Get import conditions
	export, found := c.exports.Get(sliceKey.ExportKey.String())
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
	ok, reason = export.ImportConditions.Evaluate(now, sliceKey.FileKey.OpenedAt(), fileStats.Total)
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

func (c *checker) watchExports(ctx context.Context, wg *sync.WaitGroup) (errCh <-chan error) {
	c.exports, errCh = etcdop.
		SetupMirror(
			c.logger,
			c.schema.Configs().Exports().GetAllAndWatch(ctx, c.etcdClient, etcd.WithPrevKV()),
			func(_ *op.KeyValue, export model.ExportBase) string {
				return export.ExportKey.String()
			},
			func(_ *op.KeyValue, export model.ExportBase) cachedExport {
				return cachedExport{
					ExportKey:        export.ExportKey,
					ImportConditions: export.ImportConditions,
				}
			},
		).
		WithFilter(func(event etcdop.WatchEventT[model.ExportBase]) bool {
			return c.dist.MustCheckIsOwner(event.Value.ReceiverKey.String())
		}).
		StartMirroring(wg)

	// Invalidate cache on distribution cache.
	// See WithFilter above, ownership is changed on distribution change, so cache must be re-generated.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.dist.OnChangeListener().C:
				c.exports.Restart()
			case <-ctx.Done():
				return
			}
		}
	}()

	return errCh
}

func (c *checker) watchActiveSlices(ctx context.Context, wg *sync.WaitGroup) (errCh <-chan error) {
	c.activeSlices, errCh = etcdop.
		SetupMirror(
			c.logger,
			c.schema.Slices().Writing().GetAllAndWatch(ctx, c.etcdClient, etcd.WithPrevKV()),
			func(_ *op.KeyValue, slice model.Slice) string {
				return slice.SliceKey.String()
			},
			func(_ *op.KeyValue, slice model.Slice) cachedSlice {
				return cachedSlice{
					SliceKey:       slice.SliceKey,
					CredExpiration: getCredentialsExpiration(slice),
				}
			},
		).
		WithFilter(func(event etcdop.WatchEventT[model.Slice]) bool {
			return c.dist.MustCheckIsOwner(event.Value.ReceiverKey.String())
		}).
		StartMirroring(wg)

	// Invalidate cache on distribution change.
	// See WithFilter above, ownership is changed on distribution change, so cache must be re-generated.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.dist.OnChangeListener().C:
				c.activeSlices.Restart()
			case <-ctx.Done():
				return
			}
		}
	}()

	return errCh
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
