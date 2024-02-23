package op

import "go.etcd.io/etcd/client/v3/concurrency"

// NotLockedError occurs if the lock is not locked during the entire operation.
// See AtomicOp.RequireLock for details.
type NotLockedError struct{}

// LockedError occurs if the lock is locked by another session.
// See AtomicOp.RequireLock for details.
type LockedError struct{}

func (e NotLockedError) Error() string {
	return "lock is not locked"
}

func (e LockedError) Error() string {
	return "lock is locked by another session"
}

func (e LockedError) Unwrap() error {
	return concurrency.ErrLocked
}
