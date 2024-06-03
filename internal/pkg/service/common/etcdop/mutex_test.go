package etcdop

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestMutex_LockUnlock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func TestMutex_ParallelWork(t *testing.T) {
	t.Parallel()

	type testCase struct {
		// Sessions - count on parallel sessions - virtual nodes.
		Sessions int
		// UniqueLocks - count of tested unique locks, each lock is tested in all sessions, in parallel.
		UniqueLocks int
		// Parallel - count of parallel operations per each unique lock and session.
		// Total number of goroutines is: Sessions * UniqueLocks * Parallel.
		Parallel int
		// Serial defines how many times in a row one lock is tested, in each parallel cell.
		Serial int
	}

	cases := []testCase{
		{
			Sessions:    1,
			UniqueLocks: 1,
			Parallel:    1,
			Serial:      1,
		},
		{
			Sessions:    50,
			UniqueLocks: 3,
			Parallel:    3,
			Serial:      3,
		},
		{
			Sessions:    3,
			UniqueLocks: 3,
			Parallel:    50,
			Serial:      3,
		},
		{
			Sessions:    3,
			UniqueLocks: 50,
			Parallel:    3,
			Serial:      3,
		},
		{
			Sessions:    3,
			UniqueLocks: 3,
			Parallel:    3,
			Serial:      50,
		},
	}

	for _, tc := range cases {
		name := fmt.Sprintf(`Sessions%02d_UniqueLocks%02d_Parallel%02d_Serial%02d`, tc.Sessions, tc.UniqueLocks, tc.Parallel, tc.Serial)
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			logger := log.NewDebugLogger()
			etcdCfg := etcdhelper.TmpNamespace(t)

			type lockTester struct {
				LockName string
				// CriticalWork is called from all parallel sessions,
				// but if it works correctly, there is always only one call active.
				CriticalWork func(mtx *Mutex)
			}

			total := atomic.NewInt64(0) // count and verify that all operations have been invoked
			var lockTesters []lockTester
			for i := 0; i < tc.UniqueLocks; i++ {
				active := atomic.NewInt64(0) // count of active CriticalWork calls per lock, should be always at most one
				lockTesters = append(lockTesters, lockTester{
					LockName: fmt.Sprintf("locks/my-lock-%02d", i+1),
					CriticalWork: func(mtx *Mutex) {
						require.NoError(t, mtx.Lock(ctx))
						assert.Equal(t, int64(1), active.Add(1))  // !!!
						<-time.After(1 * time.Millisecond)        // simulate some work
						assert.Equal(t, int64(0), active.Add(-1)) // !!!
						require.NoError(t, mtx.Unlock(ctx))
						total.Add(1)
					},
				})
			}

			start := make(chan struct{})
			readyWg := &sync.WaitGroup{}
			sessionWg := &sync.WaitGroup{}

			// Start N sessions
			workWg := &sync.WaitGroup{}
			for i := 0; i < tc.Sessions; i++ {
				workWg.Add(1)
				readyWg.Add(1)
				go func() {
					defer workWg.Done()

					// Create client and session
					client := etcdhelper.ClientForTest(t, etcdCfg)
					session, errCh := NewSessionBuilder().Start(ctx, sessionWg, logger, client)
					readyWg.Done()
					require.NotNil(t, session)
					require.NoError(t, <-errCh)

					// Create N unique locks in the session
					locksWg := &sync.WaitGroup{}
					for _, lockTester := range lockTesters {
						// Use each lock N times in parallel
						for k := 0; k < tc.Parallel; k++ {
							workWg.Add(1)
							locksWg.Add(1)
							go func() {
								defer workWg.Done()
								defer locksWg.Done()

								// Start all work at the same time
								select {
								case <-ctx.Done():
									require.Fail(t, "timeout when waiting for the start channel")
								case <-start:
									// continue
								}

								// Use each lock N time sequentially
								for l := 0; l < tc.Serial; l++ {
									lockTester.CriticalWork(session.NewMutex(lockTester.LockName))
								}
							}()
						}
					}

					// There is no memory leak
					locksWg.Wait()
					assert.Len(t, session.mutexStore.all, 0)
				}()
			}

			// Wait for initialization of all mutexes and start parallel work
			readyWg.Wait()
			close(start)

			// Wait for all goroutines
			workWg.Wait()

			// Close session
			cancel()
			sessionWg.Wait()

			// Check CriticalWork total calls count
			assert.Equal(t, int64(tc.Sessions*tc.UniqueLocks*tc.Parallel*tc.Serial), total.Load()) // !!!
		})
	}
}
