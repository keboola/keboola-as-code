package closesync_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestSourceAndCoordinatorNodes(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	client := mock.TestEtcdClient()

	// Create 3 source nodes and 1 coordinator node
	coordinator, err := closesync.NewCoordinatorNode(d)
	require.NoError(t, err)
	assert.Equal(t, closesync.NoSourceNode, coordinator.MinRevInUse())
	s1, err := closesync.NewSourceNode(d, "source-node-1")
	require.NoError(t, err)
	s2, err := closesync.NewSourceNode(d, "source-node-2")
	require.NoError(t, err)
	s3, err := closesync.NewSourceNode(d, "source-node-3")
	require.NoError(t, err)

	// Helper
	waitForMinRevInUse := func(t *testing.T, r int64) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, r, coordinator.MinRevInUse())
		}, 10*time.Second, 10*time.Millisecond)
	}
	waitForEtcdState := func(t *testing.T, expected string) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			etcdhelper.AssertKVsString(c, client, expected)
		}, 10*time.Second, 10*time.Millisecond)
	}

	// Check initial etcd state
	waitForEtcdState(t, `
<<<<<
runtime/closesync/source/node/source-node-1 (lease)
-----
0
>>>>>

<<<<<
runtime/closesync/source/node/source-node-2 (lease)
-----
0
>>>>>

<<<<<
runtime/closesync/source/node/source-node-3 (lease)
-----
0
>>>>>
`)
	assert.Equal(t, int64(0), coordinator.MinRevInUse())

	// The progress of individual source nodes is different
	assert.NoError(t, s1.Notify(ctx, 100))
	assert.NoError(t, s2.Notify(ctx, 101))
	assert.NoError(t, s3.Notify(ctx, 102))
	waitForMinRevInUse(t, 100)
	waitForEtcdState(t, `
<<<<<
runtime/closesync/source/node/source-node-1 (lease)
-----
100
>>>>>

<<<<<
runtime/closesync/source/node/source-node-2 (lease)
-----
101
>>>>>

<<<<<
runtime/closesync/source/node/source-node-3 (lease)
-----
102
>>>>>
`)

	// Revisions <= 100 are unblocked
	select {
	case <-coordinator.WaitForRevisionChan(99):
	default:
		assert.Fail(t, "channel should be closed")
	}
	select {
	case <-coordinator.WaitForRevisionChan(100):
	default:
		assert.Fail(t, "channel should be closed")
	}

	// Revisions > 100 are blocked
	wait101 := coordinator.WaitForRevisionChan(101)
	wait102 := coordinator.WaitForRevisionChan(102)
	wait103 := coordinator.WaitForRevisionChan(103)
	select {
	case <-wait101:
		assert.Fail(t, "channel shouldn't be closed")
	case <-wait102:
		assert.Fail(t, "channel shouldn't be closed")
	case <-wait103:
		assert.Fail(t, "channel shouldn't be closed")
	default:
	}

	// Unblock 101
	require.NoError(t, s1.Notify(ctx, 200))
	waitForMinRevInUse(t, 101)
	select {
	case <-wait101:
	default:
		assert.Fail(t, "channel should be closed")
	}
	select {
	case <-wait102:
		assert.Fail(t, "channel shouldn't be closed")
	case <-wait103:
		assert.Fail(t, "channel shouldn't be closed")
	default:
	}

	// Unblock 102
	require.NoError(t, s2.Notify(ctx, 200))
	waitForMinRevInUse(t, 102)
	select {
	case <-wait102:
	default:
		assert.Fail(t, "channel should be closed")
	}
	select {
	case <-wait103:
		assert.Fail(t, "channel shouldn't be closed")
	default:
	}

	// Unblock 103
	require.NoError(t, s3.Notify(ctx, 200))
	waitForMinRevInUse(t, 200)
	select {
	case <-wait103:
	default:
		assert.Fail(t, "channel should be closed")
	}

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()
	waitForEtcdState(t, ``)

	// No error is logged
	mock.DebugLogger().AssertNoErrorMessage(t)
}
