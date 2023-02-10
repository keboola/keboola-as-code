package etcdop

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestResistantSession(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wg := &sync.WaitGroup{}

	// Create etcd cluster for test
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1, UseBridge: true})
	defer cluster.Terminate(t)
	cluster.WaitLeader(t)
	member := cluster.Members[0]
	client := cluster.Client(0)

	// Setup resistant session
	logger := log.NewDebugLogger()
	ttlSeconds := 1
	assert.NoError(t, <-ResistantSession(ctx, wg, logger, client, ttlSeconds, func(session *concurrency.Session) error {
		logger.Info("----> new session")
		return nil
	}))

	// Drop connection for 7 seconds (dial timeout is 5 seconds)
	member.Bridge().PauseConnections()
	member.Bridge().DropConnections()
	time.Sleep(7 * time.Second)
	member.Bridge().UnpauseConnections()

	// Stop and check logs
	cancel()
	wg.Wait()
	wildcards.Assert(t, `
[etcd-session]INFO  creating etcd session
[etcd-session]INFO  created etcd session | %s
INFO  ----> new session
[etcd-session]INFO  re-creating etcd session, backoff delay %s
[etcd-session]INFO  created etcd session | %s
INFO  ----> new session
[etcd-session]INFO  closing etcd session
[etcd-session]INFO  closed etcd session | %s
`, logger.AllMessages())
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
