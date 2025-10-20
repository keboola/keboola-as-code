// Package writesync providers synchronization of in-memory data to disk or OS disk cache.
// It is also possible to wait for the next synchronization.
package writesync

import (
	"context"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Syncer according to the Config calls the Pipeline.Flush() or Pipeline.Sync().
// Regarding waiting for sync, see Notifier methods.
type Syncer struct {
	logger   log.Logger
	clock    clockwork.Clock
	config   Config
	pipeline Pipeline

	cancel    context.CancelCauseFunc
	cancelled <-chan struct{}
	wg        *sync.WaitGroup

	statistics StatisticsProvider

	// Snapshots of the metrics at the last sync

	acceptedWritesSnapshot   *atomic.Uint64
	uncompressedSizeSnapshot *atomic.Uint64
	compressedSizeSnapshot   *atomic.Uint64
	lastSyncAt               *atomic.Time

	// syncLock ensures that only one sync runs at a time
	syncLock *sync.Mutex

	// notifierLock protects the notifier field during a swap on sync
	notifierLock *sync.RWMutex
	notifier     *notify.Notifier

	// syncFn is operation triggered on sync
	syncFn func(ctx context.Context) error
}

type SyncerFactory interface {
	NewSyncer(ctx context.Context, logger log.Logger, clock clockwork.Clock, config Config, pipeline Pipeline, statistics StatisticsProvider) *Syncer
}

type DefaultSyncerFactory struct{}

func (DefaultSyncerFactory) NewSyncer(ctx context.Context, logger log.Logger, clock clockwork.Clock, config Config, pipeline Pipeline, statistics StatisticsProvider) *Syncer {
	return NewSyncer(ctx, logger, clock, config, pipeline, statistics)
}

// Pipeline is a resource that should be synchronized.
type Pipeline interface {
	// Flush data from memory to OS disk cache. Used if Config.Mode=ModeToCache.
	Flush(ctx context.Context) error
	// Sync data from memory to disk. Used if Config.Mode=ModeToDisk.
	Sync(ctx context.Context) error
}

type StatisticsProvider interface {
	// AcceptedWrites returns count of write operations waiting for the sync.
	AcceptedWrites() uint64
	// CompressedSize written to the file, measured after compression writer.
	CompressedSize() datasize.ByteSize
	// UncompressedSize written to the file, measured before compression writer.
	UncompressedSize() datasize.ByteSize
}

func NewSyncer(
	ctx context.Context,
	logger log.Logger,
	clock clockwork.Clock,
	config Config,
	pipeline Pipeline,
	statistics StatisticsProvider,
) *Syncer {
	// Select sync function
	var syncFn func(ctx context.Context) error
	switch config.Mode {
	case ModeDisk:
		syncFn = pipeline.Sync
	case ModeCache:
		syncFn = pipeline.Flush
	default:
		panic(errors.Errorf(`unexpected sync mode "%s"`, config.Mode))
	}

	s := &Syncer{
		logger:                   logger,
		clock:                    clock,
		config:                   config,
		pipeline:                 pipeline,
		wg:                       &sync.WaitGroup{},
		statistics:               statistics,
		acceptedWritesSnapshot:   atomic.NewUint64(0),
		uncompressedSizeSnapshot: atomic.NewUint64(0),
		compressedSizeSnapshot:   atomic.NewUint64(0),
		lastSyncAt:               atomic.NewTime(clock.Now()),
		syncLock:                 &sync.Mutex{},
		notifierLock:             &sync.RWMutex{},
		notifier:                 notify.New(),
		syncFn:                   syncFn,
	}

	// Syncer must be stopped by the Stop method
	ctx, s.cancel = context.WithCancelCause(ctx)
	s.cancelled = ctx.Done()

	s.logger.Debugf(
		ctx,
		`sync is enabled, mode=%s, sync each {count=%d or uncompressed=%s or compressed=%s or interval=%s}, check each %s`,
		config.Mode,
		config.CountTrigger,
		config.UncompressedBytesTrigger,
		config.CompressedBytesTrigger,
		config.IntervalTrigger,
		config.CheckInterval,
	)
	s.syncLoop()
	return s
}

// Notifier to wait for the next sync.
func (s *Syncer) Notifier(ctx context.Context) *notify.Notifier {
	// Wait is disabled, return nil notifier, *notify.Notifier(nil).Wait() is valid NOP call.
	if !s.config.Wait {
		return nil
	}

	s.notifierLock.RLock()
	s.logger.Debug(ctx, "notifier obtained")
	notifier := s.notifier
	s.notifierLock.RUnlock()
	return notifier
}

// TriggerSync initiates synchronization.
// If force=true, it waits for a running synchronization, if there is one, and then starts a new one.
// If force=false, it doesn't wait, a notifier for the running synchronization returns.
// In both cases, the method doesn't wait for the synchronization to complete,
// you can use the Wait() method of the returned *notify.Notifier for waiting.
func (s *Syncer) TriggerSync(force bool) *notify.Notifier {
	// Acquire the syncLock
	if force {
		s.syncLock.Lock()
	} else if !s.syncLock.TryLock() {
		// Skip trigger, if a sync is already in progress
		s.notifierLock.RLock()
		notifier := s.notifier
		s.notifierLock.RUnlock()
		return notifier
	}

	// At this point the syncLock is locked.
	// It is released at the end of the goroutine bellow.

	// Swap sync notifier, split old and new writes
	s.notifierLock.Lock()
	notifier := s.notifier
	s.notifier = notify.New()
	s.notifierLock.Unlock()

	acceptedWritesSnapshot := s.statistics.AcceptedWrites()
	uncompressedSizeSnapshot := uint64(s.statistics.UncompressedSize())
	compressedSizeSnapshot := uint64(s.statistics.CompressedSize())

	// Run sync in the background
	s.wg.Go(func() {
		defer s.syncLock.Unlock()

		ctx := context.Background()

		// Invoke the operation
		s.logger.Debugf(ctx, `starting sync to %s`, s.config.Mode)
		err := s.syncFn(ctx)
		if err == nil {
			// Update counters
			s.acceptedWritesSnapshot.Store(acceptedWritesSnapshot)
			s.uncompressedSizeSnapshot.Store(uncompressedSizeSnapshot)
			s.compressedSizeSnapshot.Store(compressedSizeSnapshot)
			s.lastSyncAt.Store(s.clock.Now())
			s.logger.Debugf(ctx, `sync to %s done`, s.config.Mode)
		} else {
			s.logger.Errorf(ctx, `sync to %s failed: %s`, s.config.Mode, err)
		}

		// Unblock waiting operations, see Notifier.Wait() method
		notifier.Done(err)
	})

	return notifier
}

// Stop periodical synchronization.
func (s *Syncer) Stop(ctx context.Context) error {
	s.logger.Debug(ctx, `stopping syncer`)

	// Prevent new syncs
	if s.isCancelled() {
		return errors.New(`syncer is already stopped`)
	}
	s.cancel(errors.New("syncer stopped"))

	// Run the last sync
	err := s.TriggerSync(true).Wait(ctx)

	// Wait for sync loop and running sync, if any
	s.wg.Wait()

	s.logger.Debug(ctx, `syncer stopped`)
	return err
}

func (s *Syncer) isCancelled() bool {
	select {
	case <-s.cancelled:
		return true
	default:
		return false
	}
}

func (s *Syncer) syncLoop() {
	ticker := s.clock.NewTicker(s.config.CheckInterval.Duration())

	s.wg.Go(func() {
		defer ticker.Stop()

		// Periodically check the conditions and start synchronization if any condition is met
		for {
			select {
			case <-s.cancelled:
				// The Close method has been called
				return
			case <-ticker.Chan():
				if s.checkSyncConditions() {
					s.TriggerSync(false)
				}
			}
		}
	})
}

func (s *Syncer) checkSyncConditions() bool {
	count := s.statistics.AcceptedWrites() - s.acceptedWritesSnapshot.Load()
	if count == 0 {
		return false
	}

	if count >= uint64(s.config.CountTrigger) {
		return true
	}

	uncompressedSize := s.statistics.UncompressedSize() - datasize.ByteSize(s.uncompressedSizeSnapshot.Load())
	if uncompressedSize >= s.config.UncompressedBytesTrigger {
		return true
	}

	compressedSize := s.statistics.CompressedSize() - datasize.ByteSize(s.compressedSizeSnapshot.Load())
	if compressedSize >= s.config.CompressedBytesTrigger {
		return true
	}

	durationFromLastSync := s.clock.Now().Sub(s.lastSyncAt.Load())
	return durationFromLastSync >= s.config.IntervalTrigger.Duration()
}
