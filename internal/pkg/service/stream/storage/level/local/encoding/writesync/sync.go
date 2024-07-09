// Package writesync providers synchronization of in-memory data to disk or OS disk cache.
// It is also possible to wait for the next synchronization.
package writesync

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Syncer according to the Config calls the Chain.Flush() or Chain.Sync().
// Regarding waiting for sync, see Notifier methods.
type Syncer struct {
	logger log.Logger
	clock  clock.Clock
	config Config
	chain  Chain

	stopped chan struct{}
	wg      *sync.WaitGroup

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

	// opFn is operation triggered on sync
	opFn func(ctx context.Context) error
}

type SyncerFactory func(
	ctx context.Context,
	logger log.Logger,
	clock clock.Clock,
	config Config,
	chain Chain,
	statistics StatisticsProvider,
) *Syncer

// Chain is a resource that should be synchronized.
type Chain interface {
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
	clock clock.Clock,
	config Config,
	chain Chain,
	statistics StatisticsProvider,
) *Syncer {
	// Validate config, it should be already validated
	if err := config.Validate(); err != nil {
		panic(err)
	}

	// Select sync function
	var opFn func(ctx context.Context) error
	switch config.Mode {
	case ModeDisabled:
		opFn = nil
	case ModeDisk:
		opFn = chain.Sync
	case ModeCache:
		opFn = chain.Flush
	default:
		panic(errors.Errorf(`unexpected sync mode "%s"`, config.Mode))
	}

	s := &Syncer{
		logger:                   logger,
		clock:                    clock,
		config:                   config,
		chain:                    chain,
		stopped:                  make(chan struct{}),
		wg:                       &sync.WaitGroup{},
		statistics:               statistics,
		acceptedWritesSnapshot:   atomic.NewUint64(0),
		uncompressedSizeSnapshot: atomic.NewUint64(0),
		compressedSizeSnapshot:   atomic.NewUint64(0),
		lastSyncAt:               atomic.NewTime(clock.Now()),
		syncLock:                 &sync.Mutex{},
		notifierLock:             &sync.RWMutex{},
		notifier:                 notify.New(),
		opFn:                     opFn,
	}

	// Syncer must be stopped by the Stop method
	ctx = context.WithoutCancel(ctx)

	if opFn != nil {
		s.logger.Infof(
			ctx,
			`sync is enabled, mode=%s, sync each {count=%d or uncompressed=%s or compressed=%s or interval=%s}, check each %s`,
			config.Mode,
			config.CountTrigger,
			config.UncompressedBytesTrigger,
			config.CompressedBytesTrigger,
			config.IntervalTrigger,
			config.CheckInterval,
		)
		s.syncLoop(ctx)
	} else {
		logger.Info(ctx, "sync is disabled")
	}

	return s
}

// Notifier to wait for the next sync.
func (s *Syncer) Notifier() *notify.Notifier {
	// Wait is disabled, return nil notifier, *notify.Notifier(nil).Wait() is valid NOP call.
	if !s.config.Wait {
		return nil
	}

	s.notifierLock.RLock()
	notifier := s.notifier
	s.notifierLock.RUnlock()
	return notifier
}

// TriggerSync initiates synchronization.
// If force=true, it waits for a running synchronization, if there is one, and then starts a new one.
// If force=false, is doesn't wait, a notifier for the running synchronization returns.
// In both cases, the method doesn't wait for the synchronization to complete,
// you can use the Wait() method of the returned *notify.Notifier for waiting.
func (s *Syncer) TriggerSync(ctx context.Context, force bool) *notify.Notifier {
	// Check if the sync is disabled
	if s.opFn == nil {
		return nil
	}

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

	// Update counters
	s.acceptedWritesSnapshot.Store(s.statistics.AcceptedWrites())
	s.uncompressedSizeSnapshot.Store(uint64(s.statistics.UncompressedSize()))
	s.compressedSizeSnapshot.Store(uint64(s.statistics.CompressedSize()))
	s.lastSyncAt.Store(s.clock.Now())

	// Swap sync notifier, split old and new writes
	s.notifierLock.Lock()
	notifier := s.notifier
	s.notifier = notify.New()
	s.notifierLock.Unlock()

	// Run sync in the background
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Invoke the operation
		s.logger.Debugf(ctx, `starting sync to %s`, s.config.Mode)
		err := s.opFn(ctx)
		if err == nil {
			s.logger.Debugf(ctx, `sync to %s done`, s.config.Mode)
		} else {
			s.logger.Errorf(ctx, `sync to %s failed: %s`, s.config.Mode, err)
		}

		// Release the lock
		s.syncLock.Unlock()

		// Unblock waiting operations, see Notifier.Wait() method
		notifier.Done(err)
	}()

	return notifier
}

// Stop periodical synchronization.
func (s *Syncer) Stop(ctx context.Context) error {
	s.logger.Debug(ctx, `stopping syncer`)

	// Prevent new syncs
	if s.isStopped() {
		return errors.New(`syncer is already stopped`)
	}
	close(s.stopped)

	// Run the last sync
	err := s.TriggerSync(ctx, true).Wait()

	// Wait for sync loop and running sync, if any
	s.wg.Wait()

	s.logger.Debug(ctx, `syncer stopped`)
	return err
}

func (s *Syncer) isStopped() bool {
	select {
	case <-s.stopped:
		return true
	default:
		return false
	}
}

func (s *Syncer) syncLoop(ctx context.Context) {
	ticker := s.clock.Ticker(s.config.CheckInterval.Duration())

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer ticker.Stop()

		// Periodically check the conditions and start synchronization if any condition is met
		for {
			select {
			case <-s.stopped:
				// The Close method has been called
				return
			case <-ticker.C:
				if s.checkSyncConditions() {
					s.TriggerSync(ctx, false)
				}
			}
		}
	}()
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
