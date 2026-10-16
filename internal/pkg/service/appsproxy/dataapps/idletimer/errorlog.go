package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// recordErrorLogInterval bounds how often one kind of record failure is logged.
// A missing RBAC rule or CRD fails for every workload on every round, so logging
// per workload buries the line that says what is wrong. Logging it only once
// would be worse: this log is the only signal that the records are failing, so
// it has to keep saying so until someone fixes it.
const recordErrorLogInterval = time.Minute

// errorLog reports a failure at most once per interval per reason. The round is
// a single goroutine but the sweeper writes sweepParallelism wide, so the lock
// is what lets both share one of these.
type errorLog struct {
	clock  clockwork.Clock
	logger log.Logger

	lock sync.Mutex
	last map[metav1.StatusReason]time.Time
}

func newErrorLog(clock clockwork.Clock, logger log.Logger) *errorLog {
	return &errorLog{
		clock:  clock,
		logger: logger,
		last:   map[metav1.StatusReason]time.Time{},
	}
}

func (l *errorLog) Warnf(ctx context.Context, err error, format string, a ...any) {
	if !l.due(k8serrors.ReasonForError(err)) {
		return
	}
	l.logger.Warnf(ctx, format, a...)
}

func (l *errorLog) due(reason metav1.StatusReason) bool {
	now := l.clock.Now()

	l.lock.Lock()
	defer l.lock.Unlock()

	if last, logged := l.last[reason]; logged && now.Sub(last) < recordErrorLogInterval {
		return false
	}
	l.last[reason] = now

	return true
}
