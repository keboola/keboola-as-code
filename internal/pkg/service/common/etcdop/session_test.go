package etcdop

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSession_Retries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	wg := &sync.WaitGroup{}

	// Get credentials
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Setup proxy to drop etcd connection
	proxy := toxiproxy.NewProxy(toxiproxy.NewServer(toxiproxy.NewMetricsContainer(nil), zerolog.New(os.Stderr)), "etcd-bridge", "", etcdCfg.Endpoint)
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
		WithTTLSeconds(15).
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
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"etcd session canceled"}
{"level":"info","message":"creating etcd session"}
{"level":"info","message":"cannot create etcd session: context deadline exceeded"}
{"level":"info","message":"waiting %s before the retry"}
`)
	}, 30*time.Second, 100*time.Millisecond)

	// There is no active session
	lowLevelSession, err = session.Session()
	require.Nil(t, lowLevelSession)
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

func TestSessionBackoff(t *testing.T) {
	t.Parallel()

	b := newSessionBackoff()
	b.RandomizationFactor = 0

	// Get all delays without sleep
	var delays []time.Duration
	for range 14 {
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
