package sinklock

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// LockSinkFileOperations locks all file operations in the sink.
func LockSinkFileOperations(ctx context.Context, locks *distlock.Provider, logger log.Logger, sinkKey key.SinkKey) (lock *etcdop.Mutex, unlock func()) {
	lock = locks.NewMutex(fmt.Sprintf("operator.sink.file.%s", sinkKey))
	if err := lock.Lock(ctx); err != nil {
		logger.Errorf(ctx, "cannot lock %q: %s", lock.Key(), err)
		return lock, nil
	}
	return lock, func() {
		if err := lock.Unlock(ctx); err != nil {
			logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}
}
