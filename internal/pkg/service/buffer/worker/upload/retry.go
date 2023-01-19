package upload

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

// FailedSlicesCheckInterval defines how often it will be checked
// that Slice.RetryAfter time has expired, and the upload task should be started again.
const FailedSlicesCheckInterval = time.Minute

func (u *Uploader) retryFailedUploads(ctx context.Context, wg *sync.WaitGroup, d dependencies) <-chan error {
	// Watch for failed slices.
	return orchestrator.Start(ctx, wg, d, orchestrator.Config[model.Slice]{
		Prefix:         u.schema.Slices().Failed().PrefixT(),
		ReSyncInterval: FailedSlicesCheckInterval,
		TaskType:       "slice.retry.check",
		StartTaskIf: func(event etcdop.WatchEventT[model.Slice]) (string, bool) {
			slice := event.Value
			now := model.UTCTime(u.clock.Now())
			needed := *slice.RetryAfter
			if now.After(needed) {
				return "", true
			}
			return fmt.Sprintf(`Slice.RetryAfter condition not met, now: "%s", needed: "%s"`, now, needed), false
		},
		TaskFactory: func(event etcdop.WatchEventT[model.Slice]) task.Task {
			return func(_ context.Context, logger log.Logger) (result string, err error) {
				slice := event.Value
				if err := u.store.ScheduleSliceForRetry(ctx, &slice); err != nil {
					return "", err
				}
				return "slice scheduled for retry", nil
			}
		},
	})
}

func NewRetryBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 4
	b.InitialInterval = 2 * time.Minute
	b.MaxInterval = 3 * time.Hour
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}

func RetryAt(b *backoff.ExponentialBackOff, now time.Time, attempt int) time.Time {
	max := float64(b.MaxInterval)
	interval := b.InitialInterval
	total := interval
	for i := 0; i < attempt-1; i++ {
		interval = time.Duration(math.Min(float64(interval)*b.Multiplier, max))
		total += interval
	}
	return now.Add(total)
}
