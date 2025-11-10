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
	assertClosed := func(t *testing.T, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(1 * time.Millisecond):
			assert.Fail(t, "channel should be closed")
		}
	}
	assertNotClosed := func(t *testing.T, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
			assert.Fail(t, "channel shouldn't be closed")
		default:
		}
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
	assertClosed(t, coordinator.WaitForRevisionChan(99))
	assertClosed(t, coordinator.WaitForRevisionChan(100))

	// Revisions > 100 are blocked
	wait101 := coordinator.WaitForRevisionChan(101)
	wait102 := coordinator.WaitForRevisionChan(102)
	wait103 := coordinator.WaitForRevisionChan(103)
	assertNotClosed(t, wait101)
	assertNotClosed(t, wait102)
	assertNotClosed(t, wait103)

	// Unblock 101
	require.NoError(t, s1.Notify(ctx, 200))
	waitForMinRevInUse(t, 101)
	assertClosed(t, wait101)
	assertNotClosed(t, wait102)
	assertNotClosed(t, wait103)

	// Unblock 102
	require.NoError(t, s2.Notify(ctx, 200))
	waitForMinRevInUse(t, 102)
	assertClosed(t, wait102)
	assertNotClosed(t, wait103)

	// Unblock 103
	require.NoError(t, s3.Notify(ctx, 200))
	waitForMinRevInUse(t, 200)
	assertClosed(t, wait103)

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()
	waitForEtcdState(t, ``)

	// No error is logged
	mock.DebugLogger().AssertNoErrorMessage(t)
}
