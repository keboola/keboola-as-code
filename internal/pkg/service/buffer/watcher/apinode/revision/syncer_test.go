package revision_test

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher/apinode/revision"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRevisionSyncer(t *testing.T) {
	t.Parallel()
	clk := clock.NewMock()
	d := dependencies.NewMockedDeps(t, dependencies.WithClock(clk))
	client := d.EtcdClient()

	// Create revision syncer.
	interval := 1 * time.Second
	syncer, err := revision.NewSyncer(d, "my/revision", revision.WithSyncInterval(interval))
	sync := func() {
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
	syncer.Notify(10)
	syncer.Notify(20)
	syncer.Notify(30)

	// There is no lock, so the latest revision "30" is synced.
	etcdhelper.ExpectModification(t, client, func() {
		sync()
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
30
>>>>>
`)

	// Acquire locks, we are doing some work with the current revision "30", so sync will be blocked.
	unlockRev30Lock1 := syncer.LockCurrentRevision()
	unlockRev30Lock2 := syncer.LockCurrentRevision()

	// State is updated to the revision "50", but the work based on the revision "30" is not finished yet.
	// No wand.
	syncer.Notify(40)
	syncer.Notify(50)
	sync()
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
	sync()
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
30
>>>>>
`)

	// Acquire new locks, we are doing some work with the current revision "50".
	unlockRev50Lock1 := syncer.LockCurrentRevision()
	unlockRev50Lock2 := syncer.LockCurrentRevision()

	// State is updated to the revision "70", but the work based on the revisions "30" and "50" is not finished yet.
	syncer.Notify(60)
	syncer.Notify(70)

	// Release last lock of the revision "30", so the sync is unblocked.
	// Minimal revision is use is now the revision "50".
	etcdhelper.ExpectModification(t, client, func() {
		unlockRev30Lock1()
		sync()
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
	sync()
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
		sync()
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
my/revision (lease=%s)
-----
70
>>>>>
`)

	// Etcd key should be deleted (by lease), when the API node is turned off
	etcdhelper.ExpectModification(t, client, func() {
		d.Process().Shutdown(errors.New("test shutdown"))
		d.Process().WaitForShutdown()
	})
	etcdhelper.AssertKVs(t, client, "")

	// Check logs - no unexpected syncs
	wildcards.Assert(t, `
INFO  process unique id "%s"
[watcher][api][revision][etcd-session]INFO  creating etcd session
[watcher][api][revision][etcd-session]INFO  created etcd session | %s
[watcher][api][revision]INFO  reported revision "1"
[watcher][api][revision]INFO  reported revision "30"
[watcher][api][revision]INFO  reported revision "50"
[watcher][api][revision]INFO  reported revision "70"
INFO  exiting (test shutdown)
[watcher][api][revision]INFO  received shutdown request
[watcher][api][revision]INFO  shutdown done
[watcher][api][revision][etcd-session]INFO  closing etcd session
[watcher][api][revision][etcd-session]INFO  closed etcd session | %s
INFO  exited
`, d.DebugLogger().AllMessages())
}
