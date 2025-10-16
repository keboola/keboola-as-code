package etcdop

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Mutex provides distributed locking, the interface is compatible with the concurrency.Mutex.
// The difference is that the Mutex also works as a local lock,
// so if the Lock method is called twice in a goroutine,
// the second call is blocked until the Unlock is called at first.
//
// Implementation:
//   - Mutex is only a user-facing interface to the internal struct mutexStore.
//   - The local lock is implemented via a channel, so it is possible to cancel waiting for the lock via the context.
//   - The "usage" counter prevents memory leaks, unused references are deleted.
type Mutex struct {
	store  *mutexStore
	name   string
	locked *activeMutex
}

type mutexStore struct {
	session *Session
	allLock *sync.Mutex
	all     map[string]*activeMutex
}

type activeMutex struct {
	store      *mutexStore
	name       string
	usage      int
	localMutex chan struct{}
	dbSession  *concurrency.Session
	dbMutex    *concurrency.Mutex
	teardown   chan struct{}
}

type AlreadyLockedError struct {
	reason string
}

func (e AlreadyLockedError) Error() string {
	return fmt.Sprintf("already locked: %s", e.reason)
}

func (e AlreadyLockedError) Unwrap() error {
	return concurrency.ErrLocked
}

type NotLockedError struct{}

func (e NotLockedError) Error() string {
	return "not locked"
}

func newMutexStore(session *Session) *mutexStore {
	return &mutexStore{
		session: session,
		allLock: &sync.Mutex{},
		all:     make(map[string]*activeMutex),
	}
}

// Key method match the concurrency.Mutex.Key method.
func (l *Mutex) Key() string {
	if l.locked == nil {
		return l.name
	}
	return l.locked.dbMutex.Key()
}

func (l *Mutex) IsLocked() bool {
	if l.locked == nil {
		return false
	}
	k := l.locked.dbMutex.Key()
	return k != "" && k != "\x00"
}

// Header method match the concurrency.Mutex.Header method.
// It can be used to get a fencing token, the etcd lock key revision.
func (l *Mutex) Header() *etcdserverpb.ResponseHeader {
	if l.locked == nil {
		return nil
	}
	return l.locked.dbMutex.Header()
}

// IsOwner method match the concurrency.Mutex.IsOwner method.
// It can be used to check the lock ownership in an etcd transaction.
func (l *Mutex) IsOwner() etcd.Cmp {
	if l.locked == nil {
		return etcd.Cmp{}
	}
	return l.locked.dbMutex.IsOwner()
}

// Lock locks the mutex with a cancelable context.
// Internally, the lock is backed by a local lock and an etcd mutex,
// so the protected operation runs only once within the entire cluster.
//
// You cannot use etcd as a naive locking system:
// carefully couple a fencing token, read more: https://github.com/etcd-io/etcd/issues/11457
func (l *Mutex) Lock(ctx context.Context) error {
	mtx, err := l.store.lock(ctx, l.name, func(mtx *activeMutex) error {
		// Acquire the local lock
		if err := mtx.localLock(ctx); err != nil {
			return err
		}

		// Acquire the DB lock, only at the first time
		if err := mtx.dbLock(ctx); err != nil {
			_ = mtx.localUnlock() // revert the local lock on error
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	l.locked = mtx
	return nil
}

// TryLock locks the mutex if not already locked by another session.
// If lock is held by another session, return immediately after attempting necessary cleanup
// Internally, the lock is backed by a local lock and an etcd mutex,
// so the protected operation runs only once within the entire cluster.
//
// You cannot use etcd as a naive locking system:
// carefully couple a fencing token, read more: https://github.com/etcd-io/etcd/issues/11457
func (l *Mutex) TryLock(ctx context.Context, reason string) error {
	mtx, err := l.store.lock(ctx, l.name, func(mtx *activeMutex) error {
		// Try to acquire the local lock
		if err := mtx.localTryLock(ctx, reason); err != nil {
			return err
		}

		// Try to acquire the DB lock, only at the first time
		if err := mtx.dbTryLock(ctx); err != nil {
			_ = mtx.localUnlock() // revert the local lock on error
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	l.locked = mtx
	return nil
}

func (l *Mutex) Unlock(ctx context.Context) error {
	if l.locked == nil {
		return NotLockedError{}
	}

	err := l.locked.unlock(ctx)
	l.locked = nil
	return err
}

func (s *mutexStore) NewMutex(name string) *Mutex {
	return &Mutex{store: s, name: name}
}

// get returns a mutex with the name.
func (s *mutexStore) get(name string) *activeMutex {
	s.allLock.Lock()
	defer s.allLock.Unlock()

	mtx, ok := s.all[name]
	if ok {
		// Increment usage, see clear method
		mtx.usage++
		return mtx
	}

	// Create mutex
	mtx = &activeMutex{store: s, name: name, usage: 1, localMutex: make(chan struct{}, 1)}
	s.all[name] = mtx
	return mtx
}

// getReady returns a mutex with the name, it the mutex is in teardown state, the operation is retried.
func (s *mutexStore) getReady(name string) *activeMutex {
	for {
		// Get or create mutex
		mtx := s.get(name)

		// Check if the mutex is ready
		if mtx.teardown == nil {
			return mtx
		}

		// Wait until the mutex in unlocked in DB, so it can be locked again, see clear method
		<-mtx.teardown
	}
}

// clear the mutex, if it is no more used.
func (s *mutexStore) clear(ctx context.Context, mtx *activeMutex) (err error) {
	s.allLock.Lock()
	clearMtx := false
	unlockDB := false
	if mtx.usage--; mtx.usage == 0 {
		clearMtx = true
		unlockDB = mtx.dbMutex != nil
		mtx.teardown = make(chan struct{})
	}
	s.allLock.Unlock()

	// Unlock DB if the lock is no more used
	if unlockDB {
		if err = mtx.dbMutex.Unlock(ctx); err != nil && mtx.dbSession != nil {
			// DB connection is not working, state of the lock is unknown, close session
			_ = mtx.dbSession.Close()
		}
	}

	// Clear memory
	if clearMtx {
		s.allLock.Lock()
		mtx.dbSession = nil
		mtx.dbMutex = nil
		delete(s.all, mtx.name) // prevent memory leak, remove unused mutex from the map
		close(mtx.teardown)
		s.allLock.Unlock()
	}

	return err
}

func (s *mutexStore) lock(ctx context.Context, name string, lockFn func(mtx *activeMutex) error) (mtx *activeMutex, err error) {
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	// Get or create mutex instance
	mtx = s.getReady(name)

	// Lock the mutex using the function
	err = lockFn(mtx)
	// Revert state on error
	if err != nil {
		_ = s.clear(ctx, mtx)
		return nil, err
	}

	return mtx, nil
}

func (m *activeMutex) unlock(ctx context.Context) (err error) {
	clearErr := m.store.clear(ctx, m)

	// Always unlock the local lock
	if err = m.localUnlock(); err != nil {
		return err
	}

	return clearErr
}

func (m *activeMutex) localLock(ctx context.Context) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.localMutex <- struct{}{}: // write token == locked
		return nil
	}
}

func (m *activeMutex) localTryLock(ctx context.Context, reason string) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.localMutex <- struct{}{}: // write token == locked
		return nil
	default:
		return AlreadyLockedError{reason: reason}
	}
}

func (m *activeMutex) localUnlock() error {
	select {
	case <-m.localMutex: // read token == unlocked
		return nil
	default:
		return NotLockedError{}
	}
}

func (m *activeMutex) dbLock(ctx context.Context) (err error) {
	// Get session at the first time
	if m.dbSession == nil {
		m.dbSession, err = m.store.session.WaitForSession(ctx)
		if err != nil {
			return errors.PrefixError(err, "cannot get concurrency session")
		}
	}

	// Validate session
	select {
	case <-m.dbSession.Done():
		m.dbSession = nil
		m.dbMutex = nil
		return concurrency.ErrSessionExpired
	default:
		// continue
	}

	// Acquire the DB lock, at the first time
	if m.dbMutex == nil {
		m.dbMutex = concurrency.NewMutex(m.dbSession, m.name)
		if err = m.dbMutex.Lock(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *activeMutex) dbTryLock(ctx context.Context) (err error) {
	// Get session at the first time
	if m.dbSession == nil {
		m.dbSession, err = m.store.session.Session()
		if err != nil {
			return err
		}
	}

	// Validate session
	select {
	case <-m.dbSession.Done():
		m.dbSession = nil
		m.dbMutex = nil
		return concurrency.ErrSessionExpired
	default:
		// continue
	}

	// Try to acquire the DB lock, at the first time
	if m.dbMutex == nil {
		m.dbMutex = concurrency.NewMutex(m.dbSession, m.name)
		if err = m.dbMutex.TryLock(ctx); err != nil {
			return err
		}
	}
	return err
}
