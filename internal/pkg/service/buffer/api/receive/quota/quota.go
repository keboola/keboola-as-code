// Package quota provides limitation of a receiver buffered records size in etcd.
// This will prevent etcd from filling up if data cannot be sent to file storage fast enough
// or if it is not possible to upload data at all.
package quota

import (
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	statsCache "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics/cache"
	commonErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// MinErrorLogInterval defines the minimum interval between logged quota errors, per receiver and API node.
	// It prevents repeating errors from flooding the log.
	MinErrorLogInterval = 5 * time.Minute
)

type Checker struct {
	clock         clock.Clock
	config        config.APIConfig
	cachedL2Stats *statsCache.L2

	// nextLogAt prevents errors from flooding the log
	nextLogAtLock *sync.RWMutex
	nextLogAt     map[key.ReceiverKey]time.Time
}

type dependencies interface {
	APIConfig() config.APIConfig
	Clock() clock.Clock
	StatisticsL2Cache() *statsCache.L2
}

func New(d dependencies) *Checker {
	return &Checker{
		clock:         d.Clock(),
		config:        d.APIConfig(),
		cachedL2Stats: d.StatisticsL2Cache(),
		nextLogAtLock: &sync.RWMutex{},
		nextLogAt:     make(map[key.ReceiverKey]time.Time),
	}
}

// Check checks whether the size of records that one receiver can buffer in etcd has not been exceeded.
func (c *Checker) Check(ctx context.Context, k key.ReceiverKey) error {
	stats, err := c.cachedL2Stats.ReceiverStats(ctx, k)
	if err != nil {
		return err
	}

	buffered := stats.Local.CompressedSize
	if limit := c.config.ReceiverBufferSize; buffered > limit {
		return commonErrors.NewInsufficientStorageError(c.shouldLogError(k), errors.Errorf(
			`no free space in the buffer: receiver "%s" has "%s" of compressed data buffered for upload, limit is "%s"`,
			k.ReceiverID, buffered.HumanReadable(), limit.HumanReadable(),
		))
	}

	return nil
}

// shouldLogError method determines if the quota error should be logged.
func (c *Checker) shouldLogError(k key.ReceiverKey) bool {
	now := c.clock.Now()

	c.nextLogAtLock.RLock()
	logTime := c.nextLogAt[k] // first time it returns zero time
	c.nextLogAtLock.RUnlock()

	if logTime.Before(now) {
		c.nextLogAtLock.Lock()
		c.nextLogAt[k] = now.Add(MinErrorLogInterval)
		c.nextLogAtLock.Unlock()
		return true
	}
	return false
}
