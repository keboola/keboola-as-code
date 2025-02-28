package clusterlock

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func LockFile(ctx context.Context, locks *distlock.Provider, logger log.Logger, fileKey model.FileKey) (lock *etcdop.Mutex, unlock func(), err error) {
	key := fmt.Sprintf("operator.file.%s", fileKey)
	ctx = ctxattr.ContextWith(ctx, attribute.String("lock.key", key))

	lock = locks.NewMutex(key)
	if err := lock.Lock(ctx); err != nil {
		logger.Warnf(ctx, "cannot acquire lock: %s", err)

		return nil, nil, errors.PrefixErrorf(err, "cannot acquire lock %q:", lock.Key())
	}

	logger.Debug(ctx, "acquired lock")

	unlock = func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := lock.Unlock(ctx); err != nil {
			logger.Warnf(ctx, "cannot release lock: %s", err)

			return
		}

		logger.Debug(ctx, "released lock")
	}

	return lock, unlock, nil
}
