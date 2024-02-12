package etcdop

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/shopify/toxiproxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSession_Retries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	wg := &sync.WaitGroup{}

	// Get credentials
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Setup proxy to drop etcd connection
	proxy := toxiproxy.NewProxy()
	proxy.Name = "etcd-bridge"
	proxy.Upstream = etcdCfg.Endpoint
	require.NoError(t, proxy.Start())
	defer proxy.Stop()

	// Use proxy
	etcdCfg.Endpoint = proxy.Listen

	// Create client
	client := etcdhelper.ClientForTest(t, etcdCfg)

	// Setup session
	logger := log.NewDebugLogger()
	session, errCh := NewSessionBuilder().
		WithGrantTimeout(1*time.Second).
		WithTTLSeconds(1).
		WithOnSession(func(session *concurrency.Session) error {
			require.NotNil(t, session)
			logger.Info(ctx, "----> new session (1)")
			return nil
		}).
		WithOnSession(func(session *concurrency.Session) error {
			require.NotNil(t, session)
			logger.Info(ctx, "----> new session (2)")
			return nil
		}).
		Start(ctx, wg, logger, client)
	require.NoError(t, <-errCh)
	lowLevelSession, err := session.Session()
	require.NotNil(t, lowLevelSession)
	require.NoError(t, err)

	// Drop connection
	proxy.Stop()
	assert.Eventually(t, func() bool {
		return logger.CompareJSONMessages(`
{"level":"info","message":"etcd session canceled"}
{"level":"info","message":"creating etcd session"}
{"level":"info","message":"cannot create etcd session: context deadline exceeded"}
{"level":"info","message":"waiting %s before the retry"}
`) == nil
	}, 15*time.Second, 100*time.Millisecond)

	// There is no active session
	lowLevelSession, err = session.Session()
	assert.Nil(t, lowLevelSession)
	if assert.Error(t, err) {
		assert.True(t, errors.As(err, &NoSessionError{}))
	}

	// Resume connection
	require.NoError(t, proxy.Start())

	// Wait for the new session
	_, err = session.WaitForSession(ctx)
	assert.NoError(t, err)
	lowLevelSession, err = session.Session()
	assert.NotNil(t, lowLevelSession)
	assert.NoError(t, err)

	// Stop and check logs
	cancel()
	wg.Wait()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"creating etcd session","component":"etcd.session"}
{"level":"info","message":"created etcd session","duration":"%s"}
{"level":"info","message":"----> new session (1)"}
{"level":"info","message":"----> new session (2)"}
{"level":"info","message":"etcd session canceled"}
{"level":"info","message":"creating etcd session"}
{"level":"info","message":"cannot create etcd session: context deadline exceeded"}
{"level":"info","message":"waiting %s before the retry"}
{"level":"info","message":"creating etcd session"}
{"level":"info","message":"created etcd session"}
{"level":"info","message":"----> new session (1)"}
{"level":"info","message":"----> new session (2)"}
{"level":"info","message":"closing etcd session: context canceled"}
{"level":"info","message":"closed etcd session","duration":"%s"}
`)
}

func TestSession_NewMutex(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	wg := &sync.WaitGroup{}

	// Setup
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
	lock1, err := session1.NewMutex("lock")
	require.NoError(t, err)
	lock2, err := session2.NewMutex("lock")
	require.NoError(t, err)

	// Lock - same session - ok
	require.NoError(t, lock1.TryLock(ctx))
	require.NoError(t, lock1.TryLock(ctx))
	err = lock2.TryLock(ctx)

	// Lock - different session - fail
	require.Error(t, err)
	require.True(t, errors.Is(err, concurrency.ErrLocked))

	// Unlock
	require.NoError(t, lock1.Unlock(ctx))

	// Lock - different session - ok
	require.NoError(t, lock2.TryLock(ctx))
	require.NoError(t, lock2.TryLock(ctx))

	// Close session
	cancel()
	wg.Wait()
	assert.True(t, errors.Is(lock1.TryLock(ctx), context.Canceled))
	assert.True(t, errors.Is(lock2.TryLock(ctx), context.Canceled))
}

func TestSessionBackoff(t *testing.T) {
	t.Parallel()

	b := newSessionBackoff()
	b.RandomizationFactor = 0

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 14; i++ {
		delay := b.NextBackOff()
		if delay == backoff.Stop {
			assert.Fail(t, "unexpected stop")
			break
		}
		delays = append(delays, delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		6400 * time.Millisecond,
		12800 * time.Millisecond,
		25600 * time.Millisecond,
		51200 * time.Millisecond,
		time.Minute,
		time.Minute,
		time.Minute,
	}, delays)
}
