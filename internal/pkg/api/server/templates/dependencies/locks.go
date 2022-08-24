package dependencies

import (
	"context"
	"errors"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const LockAcquireTimeout = 1 * time.Second // the lock must be acquired in 1 second, otherwise we continue without the lock
const LockReleaseTimeout = 5 * time.Second // the lock must be released in 5 second

type lockerDeps interface {
	Logger() log.Logger
	EtcdClient() (*etcd.Client, error)
}

type Locker struct {
	d   lockerDeps
	ttl int
}

type UnlockFn func()

func NewLocker(d lockerDeps, ttl int) *Locker {
	return &Locker{d: d, ttl: ttl}
}

func (l *Locker) Lock(requestCtx context.Context, lockName string) (bool, UnlockFn) {
	// Get client
	c, err := l.d.EtcdClient()
	if err != nil {
		l.d.Logger().Warnf(`cannot acquire etcd lock "%s" (continues without lock): cannot get etcd client: %s`, lockName, err.Error())
		return true, func() {}
	}

	// Get concurrency session with TTL
	session, err := concurrency.NewSession(c, concurrency.WithTTL(l.ttl))
	if err != nil {
		l.d.Logger().Warnf(`cannot acquire etcd lock "%s" (continues without lock): cannot obtain session: %s`, err.Error())
		return true, func() {}
	}

	// Context with acquire timeout
	acquireCtx, cancelFn := context.WithTimeout(requestCtx, LockAcquireTimeout)
	defer cancelFn()

	// Try lock
	mtx := concurrency.NewMutex(session, lockName)
	if err := mtx.TryLock(acquireCtx); errors.Is(err, concurrency.ErrLocked) {
		l.d.Logger().Infof(`etcd lock "%s" is used`, lockName)
		return false, func() {}
	} else if err != nil {
		l.d.Logger().Warnf("cannot acquire etcd lock (continues without lock): cannot lock mutex: %s", err.Error())
		return true, func() {}
	}

	// End the refresh for the session lease, so TTL will apply
	session.Orphan()

	// Locked, must be unlocked by returned unlock function
	l.d.Logger().Infof(`acquired etcd lock "%s"`, mtx.Key())
	return true, func() {
		releaseCtx, cancelFn := context.WithTimeout(context.Background(), LockReleaseTimeout)
		defer cancelFn()
		if err := mtx.Unlock(releaseCtx); err != nil {
			l.d.Logger().Warnf(`cannot unlock etcd lock "%s": %s`, mtx.Key(), err.Error())
		}
		if err := session.Close(); err != nil {
			l.d.Logger().Warnf(`cannot close etcd session for lock "%s": %s`, mtx.Key(), err.Error())
		}
		l.d.Logger().Infof(`released etcd lock "%s"`, lockName)
	}
}
