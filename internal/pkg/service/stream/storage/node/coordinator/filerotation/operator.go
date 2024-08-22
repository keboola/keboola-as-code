// Package filerotation provides closing of an old file, and opening of a new file, wna a configured import condition is meet.
package filerotation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type operator struct {
	config       targetConfig.OperatorConfig
	clock        clock.Clock
	logger       log.Logger
	storage      *storageRepo.Repository
	statistics   *statsCache.L1
	distribution *distribution.GroupNode
	locks        *distlock.Provider

	files *etcdop.MirrorMap[model.File, model.FileKey, *fileData]

	lock                 sync.RWMutex
	openedSlicesNotifier chan struct{}
	openedSlicesCount    map[model.FileKey]int
	fileNotEmpty         map[model.FileKey]bool
}

type fileData struct {
	FileKey       model.FileKey
	State         model.FileState
	Expiration    utctime.UTCTime
	ImportTrigger targetConfig.ImportTrigger
	Retry         model.Retryable

	// Lock prevents parallel check of the same file.
	Lock *sync.Mutex

	// Processed is true, if the entity has been modified.
	// It prevents other processing. It takes a while for the watch stream to send updated state back.
	Processed bool
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
	StatisticsL1Cache() *statsCache.L1
	DistributionNode() *distribution.Node
	DistributedLockProvider() *distlock.Provider
}

func Start(d dependencies, config targetConfig.OperatorConfig) error {
	var err error
	o := &operator{
		config:     config,
		clock:      d.Clock(),
		logger:     d.Logger().WithComponent("storage.node.operator.file.rotation"),
		storage:    d.StorageRepository(),
		statistics: d.StatisticsL1Cache(),
		locks:      d.DistributedLockProvider(),
	}

	// Join the distribution group
	{
		o.distribution, err = d.DistributionNode().Group("operator.file.rotation")
		if err != nil {
			return err
		}
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		o.logger.Info(ctx, "closing file rotation operator")

		// Stop mirroring
		cancel()
		wg.Wait()

		o.logger.Info(ctx, "closed file rotation operator")
	})

	// Mirror files
	{
		o.files = etcdop.SetupMirrorMap[model.File, model.FileKey, *fileData](
			d.StorageRepository().File().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()),
			func(_ string, file model.File) model.FileKey {
				return file.FileKey
			},
			func(_ string, file model.File, rawValue *op.KeyValue, oldValue **fileData) *fileData {
				out := &fileData{
					FileKey:       file.FileKey,
					State:         file.State,
					Expiration:    file.StagingStorage.Expiration,
					ImportTrigger: file.TargetStorage.Import.Trigger,
					Retry:         file.Retryable,
				}

				// Keep the same lock, to prevent parallel processing of the same file.
				// No modification from another code is expected, but just to be sure.
				if oldValue != nil {
					out.Lock = (*oldValue).Lock
				} else {
					out.Lock = &sync.Mutex{}
				}

				return out
			},
		).
			// Check only files owned by the node
			WithFilter(func(event etcdop.WatchEvent[model.File]) bool {
				return o.distribution.MustCheckIsOwner(event.Value.SourceKey.String())
			}).
			BuildMirror()
		if err = <-o.files.StartMirroring(ctx, wg, o.logger); err != nil {
			return err
		}
	}

	// Mirror slices, in the local storage level, so in the states: Writing, Closing, Uploading.
	// If the slice is Uploaded, it is moved to the staging storage level, so we receive the DELETE event.
	// The distribution and WithFilter is not used here, because we store only one integer per file,
	// and synchronization with the files streams can be challenging on a distribution change,
	// it is not defined which stream completes the restarted first.
	{
		o.openedSlicesNotifier = make(chan struct{})
		o.openedSlicesCount = make(map[model.FileKey]int)
		o.fileNotEmpty = make(map[model.FileKey]bool)
		slices := d.StorageRepository().Slice().GetAllInLevelAndWatch(ctx, model.LevelLocal, etcd.WithPrevKV()).
			SetupConsumer().
			WithForEach(func(events []etcdop.WatchEvent[model.Slice], header *etcdop.Header, restart bool) {
				o.lock.Lock()
				defer o.lock.Unlock()

				if restart {
					o.openedSlicesCount = make(map[model.FileKey]int)
					o.fileNotEmpty = make(map[model.FileKey]bool)
				}

				for _, event := range events {
					fileKey := event.Value.FileKey

					// Update opened slices counts, per file
					switch event.Type {
					case etcdop.CreateEvent:
						o.openedSlicesCount[fileKey]++
					case etcdop.UpdateEvent:
						// nop
					case etcdop.DeleteEvent:
						o.openedSlicesCount[fileKey]--
						if o.openedSlicesCount[fileKey] == 0 {
							delete(o.openedSlicesCount, fileKey)
						}
					}

					// Detect if all slices are empty, per file
					switch event.Type {
					case etcdop.CreateEvent, etcdop.UpdateEvent:
						if event.Value.State == model.SliceUploading {
							o.fileNotEmpty[fileKey] = o.fileNotEmpty[fileKey] || !event.Value.LocalStorage.IsEmpty
						}
					case etcdop.DeleteEvent:
						// nop
					}
				}

				// Notify
				close(o.openedSlicesNotifier)
				o.openedSlicesNotifier = make(chan struct{})
			}).
			BuildConsumer()
		if err = <-slices.StartConsumer(ctx, wg, o.logger); err != nil {
			return err
		}
	}

	// Restarts stream on distribution change
	{
		wg.Add(1)
		listener := o.distribution.OnChangeListener()

		go func() {
			defer wg.Done()
			defer listener.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case events := <-listener.C:
					o.files.Restart(errors.Errorf("distribution changed: %s", events.Messages()))
				}
			}
		}()
	}

	// Start conditions check ticker
	{
		wg.Add(1)
		ticker := d.Clock().Ticker(o.config.FileRotationCheckInterval.Duration())

		go func() {
			defer wg.Done()
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					o.checkFiles(ctx, wg)
				}
			}
		}()
	}

	return nil
}

func (o *operator) checkFiles(ctx context.Context, wg *sync.WaitGroup) {
	o.logger.Debugf(ctx, "checking files import conditions")

	o.files.ForEach(func(_ model.FileKey, file *fileData) (stop bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.checkFile(ctx, file)
		}()
		return false
	})
}

func (o *operator) checkFile(ctx context.Context, file *fileData) {
	// Prevent multiple checks of the same file
	if !file.Lock.TryLock() {
		return
	}
	defer file.Lock.Unlock()

	// File has been modified by some previous check, but we haven't received an updated version from etcd yet
	if file.Processed {
		return
	}

	if !file.Retry.Allowed(o.clock.Now()) {
		return
	}

	switch file.State {
	case model.FileWriting:
		o.rotateFile(ctx, file)
	case model.FileClosing:
		o.closeFile(ctx, file)
	default:
		// nop
	}
}

func (o *operator) rotateFile(ctx context.Context, file *fileData) {
	o.logger.Info(ctx, "rotating file")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.FileRotationTimeout.Duration())
	defer cancel()

	// Get file statistics
	stats, err := o.statistics.FileStats(ctx, file.FileKey)
	if err != nil {
		o.logger.Errorf(ctx, "cannot get file statistics: %s", err)
		return
	}

	// Check conditions
	cause, ok := shouldImport(file.ImportTrigger, o.clock.Now(), file.FileKey.OpenedAt().Time(), file.Expiration.Time(), stats.Local)
	if !ok {
		o.logger.Debugf(ctx, "skipping file rotation: %s", cause)
		return
	}

	// Rotate file
	o.logger.Infof(ctx, "rotating file for import: %s", cause)

	// Lock all file operations in the sink
	lock := o.locks.NewMutex(fmt.Sprintf("operator.sink.file.%s", file.FileKey.SinkKey))
	if err := lock.Lock(ctx); err != nil {
		o.logger.Errorf(ctx, "cannot lock %q: %s", lock.Key(), err)
		return
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			o.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}()

	// Rotate file
	err = o.storage.File().Rotate(file.FileKey.SinkKey, o.clock.Now()).RequireLock(lock).Do(ctx).Err()
	// Handle error
	if err != nil {
		o.logger.Errorf(ctx, "cannot rotate file: %s", err)

		// Increment retry delay
		err := o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment file rotation retry: %s", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true

	if err == nil {
		o.logger.Info(ctx, "successfully rotated file")
	}
}

func (o *operator) closeFile(ctx context.Context, file *fileData) {
	o.logger.Info(ctx, "closing file")

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), o.config.FileCloseTimeout.Duration())
	defer cancel()

	// Wait until all slices are uploaded
	err := o.waitForSlicesUpload(ctx, file.FileKey)
	if err != nil {
		err = errors.PrefixError(err, "error when waiting for file slices upload")
	}

	// Find if file is empty
	o.lock.RLock()
	fileEmpty := !o.fileNotEmpty[file.FileKey]
	o.lock.RUnlock()

	// Lock all file operations in the sink
	lock := o.locks.NewMutex(fmt.Sprintf("operator.sink.file.%s", file.FileKey.SinkKey))
	if err := lock.Lock(ctx); err != nil {
		o.logger.Errorf(ctx, "cannot lock %q: %s", lock.Key(), err)
		return
	}
	defer func() {
		if err := lock.Unlock(ctx); err != nil {
			o.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}()

	// Update the entity, the ctx may be cancelled
	dbCtx, dbCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer dbCancel()

	// If there is no error, switch file to the importing state
	if err == nil {
		err = o.storage.File().SwitchToImporting(file.FileKey, o.clock.Now(), fileEmpty).RequireLock(lock).Do(dbCtx).Err()
		if err != nil {
			err = errors.PrefixError(err, "cannot switch file to the importing state")
		}
	}

	// If there is an error, increment retry delay
	if err != nil {
		o.logger.Error(ctx, err.Error())
		err = o.storage.File().IncrementRetryAttempt(file.FileKey, o.clock.Now(), err.Error()).RequireLock(lock).Do(ctx).Err()
		if err != nil {
			o.logger.Errorf(ctx, "cannot increment file close retry", err)
			return
		}
	}

	// Prevents other processing, if the entity has been modified.
	// It takes a while to watch stream send the updated state back.
	file.Processed = true

	if err == nil {
		// Clean memory
		o.lock.Lock()
		defer o.lock.Unlock()
		delete(o.fileNotEmpty, file.FileKey)

		o.logger.Info(ctx, "successfully closed file")
	}
}

func (o *operator) waitForSlicesUpload(ctx context.Context, fileKey model.FileKey) error {
	for {
		// Order is important, to make it bulletproof, we have to get the notifier channel before the check
		o.lock.RLock()
		notifier := o.openedSlicesNotifier
		openedSlicesCount := o.openedSlicesCount[fileKey]
		o.lock.RUnlock()

		// Check slices openedSlicesCount
		if openedSlicesCount == 0 {
			return nil
		}

		// Wait for the next update
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifier:
			// check again
		}
	}
}
