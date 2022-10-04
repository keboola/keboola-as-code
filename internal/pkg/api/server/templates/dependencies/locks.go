package dependencies

import (
	"context"
	"errors"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const (
	LockAcquireTimeout = 2 * time.Second // the lock must be acquired in 2 seconds, otherwise we continue without the lock
	LockReleaseTimeout = 2 * time.Second // the lock must be released in 2 seconds
)

type lockerDeps interface {
	Logger() log.Logger
	EtcdClient(ctx context.Context) (*etcd.Client, error)
}

type Locker struct {
	d          lockerDeps
	ttlSeconds int
}

type UnlockFn func()

func NewLocker(d lockerDeps, ttlSeconds int) *Locker {
	return &Locker{d: d, ttlSeconds: ttlSeconds}
}

func (l *Locker) TryLock(requestCtx context.Context, lockName string) (bool, UnlockFn) {
	// Try lock
	session, mtx, err := l.tryLock(requestCtx, lockName)
	if errors.Is(err, concurrency.ErrLocked) {
		l.d.Logger().Infof(`etcd lock "%s" is used`, lockName)
		return false, func() {}
	} else if err != nil {
		l.d.Logger().Warnf(`cannot acquire etcd lock "%s" (continues without lock): %s`, lockName, err)
		return true, func() {}
	}

	// Locked, must be unlocked by returned unlock function
	l.d.Logger().Infof(`acquired etcd lock "%s"`, mtx.Key())
	return true, func() {
		releaseCtx, cancelFn := context.WithTimeout(context.Background(), LockReleaseTimeout)
		defer cancelFn()
		if err := mtx.Unlock(releaseCtx); err != nil {
			l.d.Logger().Warnf(`cannot unlock etcd lock "%s": %s`, lockName, err.Error())
		}
		if err := session.Close(); err != nil {
			l.d.Logger().Warnf(`cannot close etcd session for lock "%s": %s`, lockName, err.Error())
		}
		l.d.Logger().Infof(`released etcd lock "%s"`, lockName)
	}
}

func (l *Locker) tryLock(requestCtx context.Context, lockName string) (*concurrency.Session, *concurrency.Mutex, error) {
	// Get client
	c, err := l.d.EtcdClient(requestCtx)
	if err != nil {
		return nil, nil, fmt.Errorf(`cannot get etcd client: %w`, err)
	}

	// Acquire timeout
	acquireCtx, cancelFn := context.WithTimeout(requestCtx, LockAcquireTimeout)
	defer cancelFn()

	// Creates a new lease
	lease, err := c.Grant(acquireCtx, int64(l.ttlSeconds))
	if err != nil {
		return nil, nil, fmt.Errorf(`cannot grant lease %w`, err)
	}

	// Get concurrency session with TTL
	session, err := concurrency.NewSession(c, concurrency.WithContext(requestCtx), concurrency.WithLease(lease.ID))
	if err != nil {
		return nil, nil, fmt.Errorf(`cannot obtain session: %w`, err)
	}

	// Try lock
	mtx := concurrency.NewMutex(session, lockName)
	if err := mtx.TryLock(acquireCtx); errors.Is(err, concurrency.ErrLocked) {
		return nil, nil, err
	} else if err != nil {
		return nil, nil, fmt.Errorf(`cannot lock mutex: %w`, err)
	}

	// End the refresh for the session lease, so TTL will apply
	session.Orphan()

	return session, mtx, nil
}
