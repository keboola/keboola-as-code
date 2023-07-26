package condition

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinimalCredentialsExpiration which triggers the import.
	MinimalCredentialsExpiration = time.Hour
)

type Checker struct {
	config      config.WorkerConfig
	clock       clock.Clock
	logger      log.Logger
	client      *etcd.Client
	schema      *schema.Schema
	fileManager *file.Manager
	dist        *distribution.Node
	cachedStats *statistics.L1CacheProvider

	checkLock    *sync.Mutex
	exports      *etcdop.Mirror[model.ExportBase, cachedExport]
	tokens       *etcdop.Mirror[model.Token, string]
	activeSlices *etcdop.Mirror[model.Slice, cachedSlice]
}

type cachedExport struct {
	key.ExportKey
	ImportConditions Conditions
}

type cachedSlice struct {
	key.SliceKey
	CredExpiration time.Time
}

type dependencies interface {
	WorkerConfig() config.WorkerConfig
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	FileManager() *file.Manager
	DistributionNode() *distribution.Node
	StatisticsL1Cache() *statistics.L1CacheProvider
}

func NewChecker(d dependencies) <-chan error {
	c := &Checker{
		config:      d.WorkerConfig(),
		clock:       d.Clock(),
		logger:      d.Logger().AddPrefix("[conditions]"),
		client:      d.EtcdClient(),
		schema:      d.Schema(),
		fileManager: d.FileManager(),
		dist:        d.DistributionNode(),
		cachedStats: d.StatisticsL1Cache(),
		checkLock:   &sync.Mutex{},
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		c.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		c.logger.Info("shutdown done")
	})

	// Initialize
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Initialize watchers
		startTime := c.clock.Now()
		if err := c.watch(ctx, wg); err != nil {
			errCh <- err
			close(errCh)
			return
		}
		c.logger.Infof(`initialized | %s`, c.clock.Since(startTime))

		// Start ticker
		ticker := c.clock.Ticker(c.config.CheckConditionsInterval)
		defer ticker.Stop()
		close(errCh)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.check(ctx)
			}
		}
	}()
	return errCh
}

func (c *Checker) check(ctx context.Context) {
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

		// Authorize file manager
		var fileManager *file.AuthorizedManager
		if token, found := c.tokens.Get(slice.ExportKey.String()); found {
			fileManager = c.fileManager.WithToken(token)
		} else {
			return false
		}

		// Try swap the file, it includes also swap of the slice
		importOk, reason, err := c.shouldImport(ctx, now, slice.SliceKey, slice.CredExpiration)
		if err != nil {
			c.logger.Error(err)
		} else if importOk {
			if err := fileManager.SwapFile(slice.SliceKey.FileKey, reason); err != nil {
				c.logger.Error(err)
			}
		} else if reason != "" {
			c.logger.Debugf(`skipped import of the file "%s": %s`, slice.SliceKey.FileKey, reason)
		}

		// Try swap the slice, if it didn't already happen during import
		if !importOk {
			uploadOk, reason, err := c.shouldUpload(ctx, now, slice.SliceKey)
			if err != nil {
				c.logger.Error(err)
			} else if uploadOk {
				if err := fileManager.SwapSlice(slice.SliceKey, reason); err != nil {
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

func (c *Checker) watch(ctx context.Context, wg *sync.WaitGroup) error {
	var exportsErrCh <-chan error
	var tokensErrCh <-chan error
	var slicesErrCh <-chan error

	// Mirror exports
	c.exports, exportsErrCh = etcdop.
		SetupMirror(
			c.logger,
			c.schema.Configs().Exports().GetAllAndWatch(ctx, c.client, etcd.WithPrevKV()),
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

	// Mirror tokens
	c.tokens, tokensErrCh = etcdop.
		SetupMirror(
			c.logger,
			c.schema.Secrets().Tokens().GetAllAndWatch(ctx, c.client, etcd.WithPrevKV()),
			func(_ *op.KeyValue, token model.Token) string {
				return token.ExportKey.String()
			},
			func(_ *op.KeyValue, token model.Token) string {
				return token.Token
			},
		).
		WithFilter(func(event etcdop.WatchEventT[model.Token]) bool {
			return c.dist.MustCheckIsOwner(event.Value.ReceiverKey.String())
		}).
		StartMirroring(wg)

	// Mirror slices
	c.activeSlices, slicesErrCh = etcdop.
		SetupMirror(
			c.logger,
			c.schema.Slices().Writing().GetAllAndWatch(ctx, c.client, etcd.WithPrevKV()),
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

	// Wait for initialization
	for _, errCh := range []<-chan error{exportsErrCh, tokensErrCh, slicesErrCh} {
		errs := errors.NewMultiError()
		if err := <-errCh; err != nil {
			errs.Append(err)
		}
		if errs.Len() > 0 {
			return errs
		}
	}

	// Invalidate cache on distribution change.
	// See WithFilter methods above, ownership is changed on distribution change, so cache must be re-generated.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-c.dist.OnChangeListener().C:
				c.exports.Restart()
				c.tokens.Restart()
				c.activeSlices.Restart()
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
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
