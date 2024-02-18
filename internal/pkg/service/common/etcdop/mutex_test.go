package etcdop

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"sync"
	"testing"
	"time"
)

func TestMutex_LockUnlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}

	// Create cancelled context
	cancelledContext, cancelFn := context.WithCancel(context.Background())
	cancelFn()

	// Setup client
	logger := log.NewDebugLogger()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	// Setup sessions
	session1, errCh := NewSessionBuilder().Start(ctx, wg, logger, client)
	require.NotNil(t, session1)
	require.NoError(t, <-errCh)
	session2, errCh := NewSessionBuilder().Start(ctx, wg, logger, client)
	require.NotNil(t, session2)
	require.NoError(t, <-errCh)

	// Locks
	lock1 := session1.NewMutex("lock")
	lock2 := session2.NewMutex("lock")

	// Lock with cancelled context
	if err := lock1.Lock(cancelledContext); assert.Error(t, err) {
		assert.ErrorIs(t, err, context.Canceled)
	}

	// Lock twice - fail
	require.NoError(t, lock1.TryLock(ctx))
	err := lock1.TryLock(ctx)
	if assert.Error(t, err) {
		assert.ErrorAs(t, err, &AlreadyLockedError{})
		assert.ErrorIs(t, err, concurrency.ErrLocked)
	}

	// Lock - different session - fail
	err = lock2.TryLock(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, concurrency.ErrLocked))

	// Unlock
	require.NoError(t, lock1.Unlock(ctx))

	// Lock twice - fail
	require.NoError(t, lock2.TryLock(ctx))
	err = lock2.TryLock(ctx)
	if assert.Error(t, err) {
		assert.ErrorAs(t, err, &AlreadyLockedError{})
		assert.ErrorIs(t, err, concurrency.ErrLocked)
	}
	require.NoError(t, lock2.Unlock(ctx))

	// Unlock with cancelled context
	require.NoError(t, lock1.TryLock(ctx))
	if err = lock1.Unlock(cancelledContext); assert.Error(t, err) {
		assert.ErrorIs(t, err, context.Canceled)
	}

	// Close session
	cancel()
	wg.Wait()
	assert.ErrorIs(t, lock1.TryLock(ctx), context.Canceled)
	assert.ErrorIs(t, lock2.TryLock(ctx), context.Canceled)
}
