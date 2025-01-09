// Package collector provides collecting and saving of the storage statistics.
package collector

import (
	"context"
	"sort"
	"sync"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Collector collects writers statistics and saves them to the database.
// Collection is triggered periodically and on the writer close.
type Collector struct {
	logger     log.Logger
	repository *repository.Repository
	config     statistics.SyncConfig
	nodeID     string
	wg         *sync.WaitGroup

	syncLock    *sync.Mutex
	writersLock *sync.Mutex
	writers     map[model.SliceKey]*writerSnapshot
}

type WriterEvents interface {
	OnOpen(fn func(p encoding.Pipeline) error)
	OnClose(fn func(p encoding.Pipeline, err error) error)
}

// writerSnapshot contains collected statistics from a writer.Writer.
// It is used to determine whether the statistics have changed and should be saved to the database or not.
type writerSnapshot struct {
	writer       encoding.Pipeline
	sliceKey     model.SliceKey
	initialValue statistics.Value
	value        statistics.Value
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
	StatisticsRepository() *repository.Repository
}

func Start(d dependencies, events WriterEvents, config statistics.SyncConfig, nodeID string) {
	c := &Collector{
		logger:      d.Logger().WithComponent("statistics.collector"),
		repository:  d.StatisticsRepository(),
		config:      config,
		nodeID:      nodeID,
		wg:          &sync.WaitGroup{},
		syncLock:    &sync.Mutex{},
		writersLock: &sync.Mutex{},
		writers:     make(map[model.SliceKey]*writerSnapshot),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		cancel()
		c.logger.Info(ctx, "stopping storage statistics collector")
		c.wg.Wait()
		c.logger.Info(ctx, "storage statistics stopped")
	})

	// Listen on writer events
	events.OnOpen(func(w encoding.Pipeline) error {
		// Register the writer for the periodical sync, see bellow
		k := w.SliceKey()

		initialValue, err := c.repository.LastNodeValue(k, c.nodeID).Do(ctx).ResultOrErr()
		if err != nil {
			return err
		}

		c.writersLock.Lock()
		c.writers[k] = &writerSnapshot{
			writer:       w,
			sliceKey:     k,
			initialValue: initialValue,
			value:        statistics.Value{},
		}
		c.writersLock.Unlock()

		return nil
	})
	events.OnClose(func(p encoding.Pipeline, _ error) error {
		// Sync the final statistics and unregister the writer
		k := p.SliceKey()
		err := c.syncOne(k)

		c.writersLock.Lock()
		delete(c.writers, k)
		c.writersLock.Unlock()

		if err != nil {
			c.logger.Errorf(ctx, "cannot sync stats on writer close: %s, slice %q", err, k.String())
			return err
		}

		c.logger.Debugf(ctx, "stats synced on writer close, slice %q", k.String())
		return nil
	})

	// Periodically collect statistics and sync them to the database.
	// Collector can be disabled in tests.
	// It causes problems when mocked clock is used.
	// For example clock.Add(time.Hour) invokes the timer 3600 times, if the interval is 1s.
	if c.config.Enabled {
		c.wg.Add(1)
		ticker := d.Clock().NewTicker(c.config.SyncInterval.Duration())
		go func() {
			defer c.wg.Done()
			defer ticker.Stop()

			// Note: errors are already logged
			for {
				select {
				case <-ctx.Done():
					_ = c.syncAll()
					return
				case <-ticker.Chan():
					_ = c.syncAll()
				}
			}
		}()
	}
}

func (c *Collector) syncAll() error {
	return c.sync(nil)
}

func (c *Collector) syncOne(k model.SliceKey) error {
	return c.sync(&k)
}

// sync all writers or a one writer, if the filter is specified.
func (c *Collector) sync(filter *model.SliceKey) error {
	c.syncLock.Lock()
	defer c.syncLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), c.config.SyncTimeout.Duration())
	defer cancel()

	// Collect statistics
	c.writersLock.Lock()
	var forSync []statistics.PerSlice
	if filter == nil {
		// Collect all writers
		for _, s := range c.writers {
			if changed := c.collect(s.writer, &s.value); changed {
				value := s.initialValue.With(s.value)
				forSync = append(forSync, statistics.PerSlice{
					SliceKey:         s.sliceKey,
					FirstRecordAt:    value.FirstRecordAt,
					LastRecordAt:     value.LastRecordAt,
					RecordsCount:     value.RecordsCount,
					UncompressedSize: value.UncompressedSize,
					CompressedSize:   value.CompressedSize,
				})
			}
		}
	} else if s, found := c.writers[*filter]; found {
		// Collect one writer
		if changed := c.collect(s.writer, &s.value); changed {
			value := s.initialValue.With(s.value)
			forSync = append(forSync, statistics.PerSlice{
				SliceKey:         s.sliceKey,
				FirstRecordAt:    value.FirstRecordAt,
				LastRecordAt:     value.LastRecordAt,
				RecordsCount:     value.RecordsCount,
				UncompressedSize: value.UncompressedSize,
				CompressedSize:   value.CompressedSize,
			})
		}
	}
	c.writersLock.Unlock()

	// Sort slice for easier debugging
	sort.SliceStable(forSync, func(i, j int) bool {
		return forSync[i].SliceKey.String() < forSync[j].SliceKey.String()
	})

	// Update values in the database
	if len(forSync) > 0 {
		if err := c.repository.Put(ctx, c.nodeID, forSync); err != nil {
			err = errors.Errorf("cannot save the storage statistics to the database: %w", err)
			c.logger.Error(ctx, err.Error())
			return err
		}
	}

	c.logger.Debug(ctx, "sync done")
	return nil
}

// collect statistics from the writer to the PerSlice struct.
func (c *Collector) collect(w encoding.Pipeline, out *statistics.Value) (changed bool) {
	// Get values
	firstRowAt := w.FirstRecordAt()
	lastRowAt := w.LastRecordAt()
	rowsCount := w.CompletedWrites()
	compressedSize := w.CompressedSize()
	uncompressedSize := w.UncompressedSize()

	// Are statistics changed?
	changed = out.FirstRecordAt != firstRowAt ||
		out.LastRecordAt != lastRowAt ||
		out.RecordsCount != rowsCount ||
		out.CompressedSize != compressedSize ||
		out.UncompressedSize != uncompressedSize

	// Update values
	out.FirstRecordAt = firstRowAt
	out.LastRecordAt = lastRowAt
	out.RecordsCount = rowsCount
	out.CompressedSize = compressedSize
	out.UncompressedSize = uncompressedSize

	return changed
}
