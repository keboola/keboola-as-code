// Package disksync providers synchronization of in-memory data to disk or OS disk cache.
// It is also possible to wait for the next synchronization.
package disksync

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/notify"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"go.uber.org/atomic"
	"io"
	"sync"
)

// Syncer writes data to the underlying writer and according to the Config it calls the chain.Flush() or chain.Sync().
// Regarding waiting for sync, see Notifier and DoWithNotifier methods.
type Syncer struct {
	logger log.Logger
	config Config
	writer nextWriter
	syncer chain
	timer  *clock.Timer

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

// nextWriter is an underlying writer, for example *os.File
type nextWriter interface {
	io.Writer
	io.StringWriter
}

// chain is a resource responsible for synchronizing of file writers.
type chain interface {
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
		logger:         logger,
		config:         config,
		writer:         writer,
		syncer:         syncer,
		timer:          clock.Timer(config.IntervalTrigger),
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

// Write writes to the underlying writer and trigger synchronization when the configured Config.BytesTrigger volume is exceeded.
func (s *Syncer) Write(p []byte) (n int, err error) {
	if err := s.ctx.Err(); err != nil {
		return 0, errors.Errorf(`syncer is closed: %w`, err)
	}

	// Write data to the underlying writer
	n, err = s.writer.Write(p)
	if err != nil {
		return n, err
	}

	s.onWrite(n)
	return n, nil
}

// WriteString - see Write,
// some writes are optimized for writing strings
// without an unnecessary conversion from []byte to string,
// so both methods are supported.
func (s *Syncer) WriteString(str string) (n int, err error) {
	if err := s.ctx.Err(); err != nil {
		return 0, errors.Errorf(`syncer is closed: %w`, err)
	}

	// Write data to the underlying writer
	n, err = s.writer.WriteString(str)
	if err != nil {
		return n, err
	}

	s.onWrite(n)
	return n, nil
}

// Close method stop periodical synchronization.
func (s *Syncer) Close() error {
	s.logger.Debug(`closing syncer`)
	s.cancel()
	s.wg.Wait()
	s.SyncAndWait()
	s.logger.Debug(`syncer closed`)
	return nil
}

// Notifier returns Notifier that will inform about the next sync.
// If synchronization is disabled, or waiting is disabled (Config.Wait==false),
// the *notify.Notifier(nil) value is returned,
// so the Notifier.Wait() method returns immediately.
func (s *Syncer) Notifier() *notify.Notifier {
	if s == nil || !s.config.Wait {
		// Note: *notify.Notifier(nil).Wait() is a valid call
		return nil
	}

	s.notifierLock.RLock()
	notifier := s.notifier
	s.notifierLock.RUnlock()

	return notifier
}

// DoWithNotifier guarantees that the Notifier will not change during the DO operation, the sync will not start.
// After the DO operation, notifier is returned, so you can wait for the next sync.
// If synchronization is disabled, or waiting is disabled (Config.Wait==false),
// the *notify.Notifier(nil) value is returned,
// so the Notifier.Wait() method returns immediately.
func (s *Syncer) DoWithNotifier(do func()) *notify.Notifier {
	if s == nil || !s.config.Wait {
		do()
		// Note: *notify.Notifier(nil).Wait() is a valid call
		return nil
	}

	s.notifierLock.RLock()
	notifier := s.notifier
	do()
	s.notifierLock.RUnlock()

	return notifier
}

// SyncAndWait initiates synchronization and waits for its completion.
// If a synchronization is already in progress, the method waits for it to complete before starting a new one.
// Method is public for tests.
func (s *Syncer) SyncAndWait() {
	// Wait, if the sync is already in progress
	s.syncLock.Lock()

	// Wait for sync completion
	_ = s.doSync().Wait()
}

// Sync initiates synchronization if it is not already running.
// Method is public for tests.
func (s *Syncer) Sync() {
	// Skip, if the sync is already in progress
	if !s.syncLock.TryLock() {
		return
	}

	// Run sync in the background
	s.doSync()
}

func (s *Syncer) onWrite(l int) {
	// Increment counter and check size condition
	if datasize.ByteSize(s.bytesToSync.Add(uint64(l))) >= s.config.BytesTrigger {
		s.bytesTriggerCh <- struct{}{}
	}
}

func (s *Syncer) startSyncLoop() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.timer.Stop()

		for {
			select {
			case <-s.ctx.Done():
				// The Close method has been called
				return
			case <-s.bytesTriggerCh:
				// SyncAfterBytes
				s.Sync()
			case <-s.timer.C:
				// SyncAfterInterval
				s.Sync()
			}
		}
	}()
}

func (s *Syncer) doSync() *notify.Notifier {
	// Swap sync notifier, split old and new writes
	s.notifierLock.Lock()
	notifier := s.notifier
	s.notifier = notify.New()
	s.notifierLock.Unlock()

	// Schedule next periodical sync
	s.timer.Reset(s.config.IntervalTrigger)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		// Check bytes size
		var err error
		if bytes := s.bytesToSync.Swap(0); bytes == 0 {
			// Nothing to do, skip
			s.logger.Debug(`nothing to sync`)
		} else {
			// Invoke the operation
			s.logger.Debugf(`starting sync of "%s" to %s`, datasize.ByteSize(bytes).HumanReadable(), s.config.Mode)
			err = s.opFn()
			if err == nil {
				s.logger.Debugf(`sync to %s done`, s.config.Mode)
			} else {
				s.logger.Errorf(`sync to %s failed: %s`, s.config.Mode, err)
			}
		}

		// Release the lock
		s.syncLock.Unlock()

		// Unblock waiting operations, see Notifier.Wait() method
		notifier.Done(err)
	}()

	return notifier
}
