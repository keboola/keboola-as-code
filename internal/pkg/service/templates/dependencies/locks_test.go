package dependencies

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testDeps struct {
	logger     log.DebugLogger
	etcdClient *etcd.Client
}

func (d *testDeps) Logger() log.Logger {
	return d.logger
}

func (d *testDeps) EtcdClient() *etcd.Client {
	return d.etcdClient
}

func TestLocker_WithEtcd_TimeToLiveExpired(t *testing.T) {
	t.Parallel()

	// Create locker
	d := &testDeps{logger: log.NewDebugLogger(), etcdClient: etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))}
	ttlSeconds := 2
	locker := NewLocker(d, ttlSeconds)

	// Test!
	// Project is locked
	locked1, unlock1Fn := locker.TryLock(context.Background(), "projectId=456")
	defer unlock1Fn()
	assert.True(t, locked1)
	// ... so project cannot be locked by other requests
	locked2, unlock2Fn := locker.TryLock(context.Background(), "projectId=456")
	defer unlock2Fn()
	assert.False(t, locked2)
	// ... but after ttlSeconds, lock is auto-released and project can be locked again
	time.Sleep(time.Duration(ttlSeconds+1) * time.Second)
	locked3, unlock3Fn := locker.TryLock(context.Background(), "projectId=456")
	defer unlock3Fn()
	assert.True(t, locked3)
	// Unlock
	unlock3Fn()

	// Check logged messages
	expected := `
INFO  acquired etcd lock "projectId=456/%s"
INFO  etcd lock "projectId=456" is used
INFO  acquired etcd lock "projectId=456/%s"
INFO  released etcd lock "projectId=456"
`
	wildcards.Assert(t, strings.TrimLeft(expected, "\n"), d.logger.AllMessages())
}
