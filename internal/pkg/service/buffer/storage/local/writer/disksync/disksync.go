// Package disksync providers synchronization of in-memory data to disk or OS disk cache.
// It is also possible to wait for the next synchronization.
package disksync

import (
	"context"
	"io"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Syncer writes data to the underlying writer and according to the Config it calls the chain.Flush() or chain.Sync().
// Regarding waiting for sync, see Notifier and DoWithNotifier methods.
type Syncer struct {
	logger log.Logger
	clock  clock.Clock
	config Config
	chain  chain

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup

	// writeOpsCount is updated via AddWriteOp method.
	// It contains count of high-level writers, e.g., one table row = one write operation.
	// This may not correspond to the number of calls of the low-level Write or WriteString methods.
	writeOpsCount *atomic.Uint64
	// lastSyncAt is updated on each sync start
	lastSyncAt *atomic.Time
	// bytesToSync are updated by the Write method, the Syncer is the first writer in the chain
	bytesToSync *atomic.Uint64

	// syncLock ensures that only one sync runs at a time
	syncLock *sync.Mutex

	// notifierLock ensures that each high-level write operation receives the notifier that belongs to it, see DoWithNotifier.
	notifierLock *sync.RWMutex
	notifier     *notify.Notifier

	opFn func() error
}

// chain is a resource responsible for synchronizing of file writers.
type chain interface {
	io.Writer
	io.StringWriter
	// Flush data from memory to OS disk cache. Used if Config.Mode=ModeToCache.
	Flush() error
	// Sync data from memory to disk. Used if Config.Mode=ModeToDisk.
	Sync() error
}

// NewSyncer may return nil if the synchronization is disabled by the Config.
func NewSyncer(logger log.Logger, clock clock.Clock, config Config, chain chain) *Syncer {
	// Process mode option
	var opFn func() error
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

	// Check conditions
	if config.Mode != ModeDisabled {
		if config.CheckInterval <= 0 {
			panic(errors.New("checkInterval is not set"))
		}
		if config.IntervalTrigger <= 0 {
			panic(errors.New("intervalTrigger is not set"))
		}
		if config.BytesTrigger <= 0 {
			panic(errors.New("bytesTrigger is not set"))
		}
	}

	w := &Syncer{
		logger:        logger,
		clock:         clock,
		config:        config,
		chain:         chain,
		wg:            &sync.WaitGroup{},
		writeOpsCount: atomic.NewUint64(0),
		lastSyncAt:    atomic.NewTime(clock.Now()),
		bytesToSync:   atomic.NewUint64(0),
		syncLock:      &sync.Mutex{},
		notifierLock:  &sync.RWMutex{},
		notifier:      notify.New(),
		opFn:          opFn,
	}

	w.ctx, w.cancel = context.WithCancel(context.Background())

	if opFn != nil {
		w.logger.Infof(
			`sync is enabled, mode=%s, sync each {count=%d or bytes=%s or interval=%s}, check each %s`,
			config.Mode,
			config.CountTrigger,
			config.BytesTrigger,
			config.IntervalTrigger,
			config.CheckInterval,
		)
		w.syncLoop()
	} else {
		logger.Info("sync is disabled")
	}

	return w
}

// AddWriteOp increments number of high-level writer operations,
// for example writing one row of the table is one high-level write operation.
func (s *Syncer) AddWriteOp(n uint) {
	s.writeOpsCount.Add(uint64(n))
}

// WaitingWriteOps returns count of write operations waiting for the sync, for tests.
func (s *Syncer) WaitingWriteOps() uint64 {
	return s.writeOpsCount.Load()
}

// DoWithNotify provides wrapping for multiple write operations and waiting for them to be synced to disk.
// This is ensured by shared notifierLock, so notifier cannot be swapped during the method,
// but parallel writes are not blocked. The lock blocks the TriggerSync method,
// so operations are expected to be short.
func (s *Syncer) DoWithNotify(do func() error) (notifier *notify.Notifier, err error) {
	// Get notifier and block it change during write, see doSync method
	// Note: *notify.Notifier(nil).Wait() is a valid call
	if s.config.Wait {
		s.notifierLock.RLock()
		defer s.notifierLock.RUnlock()
		notifier = s.notifier
	}

	if err = do(); err != nil {
		return nil, err
	}

	return notifier, nil
}

// WriteWithNotify writes to the underlying writer.
// Returned *notify.Notifier can be used to wait for disk sync.
func (s *Syncer) WriteWithNotify(p []byte) (n int, notifier *notify.Notifier, err error) {
	// Get notifier and block it change during write, see doSync method
	// Note: *notify.Notifier(nil).Wait() is a valid call
	if s.config.Wait {
		s.notifierLock.RLock()
		defer s.notifierLock.RUnlock()
		notifier = s.notifier
	}

	n, err = s.Write(p)
	if err != nil {
		return n, nil, err
	}

	s.AddWriteOp(1)
	return n, notifier, nil
}

func (s *Syncer) Write(p []byte) (n int, err error) {
	// Write data to the underlying writer
	n, err = s.chain.Write(p)
	if err != nil {
		return n, err
	}

	s.bytesToSync.Add(uint64(n))
	return n, nil
}

func (s *Syncer) WriteString(str string) (n int, err error) {
	// Write data to the underlying writer
	n, err = s.chain.WriteString(str)
	if err != nil {
		return n, err
	}

	s.bytesToSync.Add(uint64(n))
	return n, nil
}

// Stop periodical synchronization.
func (s *Syncer) Stop() error {
	if err := s.ctx.Err(); err != nil {
		return errors.Errorf(`syncer is already stopped: %w`, err)
	}

	s.logger.Debug(`stopping syncer`)

	// Stop sync loop
	s.cancel()

	// Run last sync
	err := s.TriggerSync(true).Wait()

	// Wait for sync loop and running sync, if any
	s.wg.Wait()

	s.logger.Debug(`syncer stopped`)
	return err
}

// TriggerSync initiates synchronization.
// If force=true, it waits for a running synchronization, if there is one, and then starts a new one.
// If force=false, is doesn't wait, a notifier for the running synchronization returns.
// In both cases, the method doesn't wait for the synchronization to complete,
// you can use the Wait() method of the returned *notify.Notifier for waiting.
func (s *Syncer) TriggerSync(force bool) *notify.Notifier {
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
		defer s.notifierLock.RUnlock()
		return s.notifier
	}

	// At this point the syncLock is locked.
	// It is released at the end of the goroutine bellow.

	// Update counters
	s.writeOpsCount.Store(0)
	s.lastSyncAt.Store(s.clock.Now())
	s.bytesToSync.Store(0)

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
		s.logger.Debugf(`starting sync to %s`, s.config.Mode)
		err := s.opFn()
		if err == nil {
			s.logger.Debugf(`sync to %s done`, s.config.Mode)
		} else {
			s.logger.Errorf(`sync to %s failed: %s`, s.config.Mode, err)
		}

		// Release the lock
		s.syncLock.Unlock()

		// Unblock waiting operations, see Notifier.Wait() method
		notifier.Done(err)
	}()

	return notifier
}

func (s *Syncer) syncLoop() {
	ticker := s.clock.Ticker(s.config.CheckInterval)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer ticker.Stop()

		// Periodically check the conditions and start synchronization if any condition is met
		for {
			select {
			case <-s.ctx.Done():
				// The Close method has been called
				return
			case <-ticker.C:
				if count := s.writeOpsCount.Load(); count > 0 {
					countTrigger := count >= uint64(s.config.CountTrigger)
					bytesTrigger := datasize.ByteSize(s.bytesToSync.Load()) >= s.config.BytesTrigger
					intervalTrigger := s.clock.Now().Sub(s.lastSyncAt.Load()) >= s.config.IntervalTrigger
					if countTrigger || bytesTrigger || intervalTrigger {
						s.TriggerSync(false)
					}
				}
			}
		}
	}()
}
