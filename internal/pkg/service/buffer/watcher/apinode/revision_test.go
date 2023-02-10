package apinode

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type statsSyncer struct {
	logger log.Logger
}

func (s *statsSyncer) Sync(_ context.Context) <-chan struct{} {
	s.logger.Debug(">>> statistics sync")
	done := make(chan struct{})
	close(done)
	return done
}

func TestRevisionSyncer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test dependencies
	wg := &sync.WaitGroup{}
	clk := clock.NewMock()
	logger := log.NewDebugLogger()
	client := etcdhelper.ClientForTest(t)

	// Create revision syncer.
	ttl := 15
	interval := 1 * time.Second
	s, err := newSyncer(ctx, wg, clk, logger, &statsSyncer{logger: logger}, client, "my/revision", ttl, interval)
	doSync := func() {
		clk.Add(interval)
	}

	// Check initial sync.
	assert.NoError(t, err)
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
1
>>>>>
`)

	// State is updated to the revision "30".
	s.Notify(10)
	s.Notify(20)
	s.Notify(30)

	// There is no lock, so the latest revision "30" is synced.
	etcdhelper.ExpectModification(t, client, func() {
		doSync()
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
30
>>>>>
`)

	// Acquire locks, we are doing some work with the current revision "30", so sync will be blocked.
	unlockRev30Lock1 := s.Lock()
	unlockRev30Lock2 := s.Lock()

	// State is updated to the revision "50", but the work based on the revision "30" is not finished yet.
	// No wand.
	s.Notify(40)
	s.Notify(50)
	doSync()
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
30
>>>>>
`)

	// Unlock "rev30Lock2", revision "30" is still locked by the "rev30Lock1".
	// No sync.
	unlockRev30Lock2()
	doSync()
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
30
>>>>>
`)

	// Acquire new locks, we are doing some work with the current revision "50".
	unlockRev50Lock1 := s.Lock()
	unlockRev50Lock2 := s.Lock()

	// State is updated to the revision "70", but the work based on the revisions "30" and "50" is not finished yet.
	s.Notify(60)
	s.Notify(70)

	// Release last lock of the revision "30", so the sync is unblocked.
	// Minimal revision is use is now the revision "50".
	etcdhelper.ExpectModification(t, client, func() {
		unlockRev30Lock1()
		doSync()
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
50
>>>>>
`)

	// Unlock "rev50Lock1", no sync, revision "50" is still locked by the "rev50Lock2"
	unlockRev50Lock1()
	doSync()
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
50
>>>>>
`)

	// Release last lock of the revision "50", so the sync is unblocked.
	// There is no revision in use, so the key is synced to the current revision "70".
	etcdhelper.ExpectModification(t, client, func() {
		unlockRev50Lock2()
		doSync()
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
70
>>>>>
`)

	// Etcd key should be deleted (by lease), when the API node is turned off
	cancel()
	wg.Wait()
	etcdhelper.AssertKVs(t, client, "")

	// Check logs - no unexpected syncs
	wildcards.Assert(t, `
[etcd-session]INFO  creating etcd session
[etcd-session]INFO  created etcd session | %s
DEBUG  >>> statistics sync
INFO  reported revision "1"
DEBUG  >>> statistics sync
INFO  reported revision "30"
INFO  locked revision "30"
INFO  locked revision "50"
INFO  unlocked revision "30"
DEBUG  >>> statistics sync
INFO  reported revision "50"
INFO  unlocked revision "50"
DEBUG  >>> statistics sync
INFO  reported revision "70"
[etcd-session]INFO  closing etcd session
[etcd-session]INFO  closed etcd session | %s
`, logger.AllMessages())
}
