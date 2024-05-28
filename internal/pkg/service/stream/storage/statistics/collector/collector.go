// Package collector provides collecting and saving of the storage statistics.
package collector

import (
	"context"
	"sort"
	"sync"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer"
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

	config statistics.SyncConfig

	cancel context.CancelFunc
	wg     *sync.WaitGroup

	syncLock    *sync.Mutex
	writersLock *sync.Mutex
	writers     map[model.SliceKey]*writerSnapshot
}

type WriterEvents interface {
	OnWriterOpen(fn func(writer.Writer) error)
	OnWriterClose(func(w writer.Writer, err error) error)
}

// writerSnapshot contains collected statistics from a writer.Writer.
// It is used to determine whether the statistics have changed and should be saved to the database or not.
type writerSnapshot struct {
	stats  statistics.PerSlice
	writer writer.Writer
}

func New(logger log.Logger, clk clock.Clock, repository *repository.Repository, events WriterEvents, config statistics.SyncConfig) *Collector {
	c := &Collector{
		logger:      logger,
		repository:  repository,
		config:      config,
		wg:          &sync.WaitGroup{},
		syncLock:    &sync.Mutex{},
		writersLock: &sync.Mutex{},
		writers:     make(map[model.SliceKey]*writerSnapshot),
	}

	// Create context for cancellation of the periodical sync
	var ctx context.Context
	ctx, c.cancel = context.WithCancel(context.Background())

	// Listen on writer events
	events.OnWriterOpen(func(w writer.Writer) error {
		// Register the writer for the periodical sync, see bellow
		k := w.SliceKey()

		c.writersLock.Lock()
		c.writers[k] = &writerSnapshot{
			writer: w,
			stats: statistics.PerSlice{
				SliceKey: k,
				Value:    statistics.Value{SlicesCount: 1},
			},
		}
		c.writersLock.Unlock()

		return nil
	})
	events.OnWriterClose(func(w writer.Writer, _ error) error {
		// Sync the final statistics and unregister the writer
		k := w.SliceKey()
		err := c.syncOne(k)

		c.writersLock.Lock()
		delete(c.writers, k)
		c.writersLock.Unlock()

		return err
	})

	// Periodically collect statistics and sync them to the database
	c.wg.Add(1)
	ticker := clk.Ticker(c.config.SyncInterval.Duration())
	go func() {
		defer c.wg.Done()
		defer ticker.Stop()

		// Note: errors are already logged
		for {
			select {
			case <-ctx.Done():
				_ = c.syncAll()
				return
			case <-ticker.C:
				_ = c.syncAll()
			}
		}
	}()

	return c
}

// Stop periodical sync on shutdown.
func (c *Collector) Stop(ctx context.Context) {
	c.cancel()
	c.logger.Info(ctx, "stopping storage statistics collector")
	c.wg.Wait()
	c.logger.Info(ctx, "storage statistics stopped")
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
			if changed := c.collect(s.writer, &s.stats); changed {
				forSync = append(forSync, s.stats)
			}
		}
	} else {
		// Collect one writer
		if s, found := c.writers[*filter]; found {
			if changed := c.collect(s.writer, &s.stats); changed {
				forSync = append(forSync, s.stats)
			}
		}
	}
	c.writersLock.Unlock()

	// Sort slice for easier debugging
	sort.SliceStable(forSync, func(i, j int) bool {
		return forSync[i].SliceKey.String() < forSync[j].SliceKey.String()
	})

	// Update values in the database
	if len(forSync) > 0 {
		if err := c.repository.Put(ctx, forSync); err != nil {
			err = errors.Errorf("cannot save the storage statistics to the database: %w", err)
			c.logger.Error(ctx, err.Error())
			return err
		}
	}

	c.logger.Debug(ctx, "sync done")
	return nil
}

// collect statistics from the writer to the PerSlice struct.
func (c *Collector) collect(w writer.Writer, out *statistics.PerSlice) (changed bool) {
	// Get values
	firstRowAt := w.FirstRowAt()
	lastRowAt := w.LastRowAt()
	rowsCount := w.RowsCount()
	compressedSize := w.CompressedSize()
	uncompressedSize := w.UncompressedSize()

	// Are statistics changed?
	changed = out.Value.FirstRecordAt != firstRowAt ||
		out.Value.LastRecordAt != lastRowAt ||
		out.Value.RecordsCount != rowsCount ||
		out.Value.CompressedSize != compressedSize ||
		out.Value.UncompressedSize != uncompressedSize

	// Update values
	out.Value.FirstRecordAt = firstRowAt
	out.Value.LastRecordAt = lastRowAt
	out.Value.RecordsCount = rowsCount
	out.Value.CompressedSize = compressedSize
	out.Value.UncompressedSize = uncompressedSize

	return changed
}
