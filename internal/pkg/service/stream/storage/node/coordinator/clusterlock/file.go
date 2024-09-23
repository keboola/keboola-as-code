package clusterlock

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func LockFile(ctx context.Context, locks *distlock.Provider, logger log.Logger, fileKey model.FileKey) (lock *etcdop.Mutex, unlock func(), err error) {
	lock = locks.NewMutex(fmt.Sprintf("operator.file.%s", fileKey))
	if err := lock.Lock(ctx); err != nil {
		return nil, nil, errors.PrefixErrorf(err, "cannot lock %q:", lock.Key())
	}

	logger.Debug(ctx, "acquired lock")

	unlock = func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := lock.Unlock(ctx); err != nil {
			logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
		}
	}

	return lock, unlock, nil
}
