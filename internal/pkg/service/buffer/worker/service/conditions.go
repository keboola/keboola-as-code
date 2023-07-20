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

	lock             *sync.RWMutex
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
		lock:             &sync.RWMutex{},
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
	c.lock.RLock()
	defer c.lock.RUnlock()

	now := c.clock.Now()
	for sliceKey, slice := range c.openedSlices {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !c.Service.dist.MustCheckIsOwner(sliceKey.ReceiverKey.String()) {
			continue
		}

		// Check credentials expiration
		if slice.expiration.Sub(now) <= MinimalCredentialsExpiration {
			reason := fmt.Sprintf("upload credentials will expire soon, at %s", slice.expiration.UTC().String())
			if err := c.swapFile(sliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		}

		// Get import conditions
		cdn, found := c.importConditions[sliceKey.ExportKey]
		if !found {
			continue
		}

		// Get file stats
		fileStats, err := c.cachedStats.FileStats(ctx, sliceKey.FileKey)
		if err != nil {
			c.logger.Error(err)
			continue
		}

		// Check import conditions
		if met, reason := cdn.Evaluate(now, sliceKey.FileKey.OpenedAt(), fileStats.Total); met {
			if err := c.swapFile(sliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
			continue
		}

		// Get slice stats
		sliceStats, err := c.cachedStats.SliceStats(ctx, sliceKey)
		if err != nil {
			c.logger.Error(err)
			continue
		}

		// Check upload conditions
		if met, reason := c.uploadConditions.Evaluate(now, sliceKey.OpenedAt(), sliceStats.Total); met {
			if err := c.swapSlice(sliceKey, reason); err != nil {
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
