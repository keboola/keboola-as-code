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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testDeps struct {
	logger     log.DebugLogger
	etcdClient *etcd.Client
}

func (d *testDeps) Logger() log.Logger {
	return d.logger
}

func (d *testDeps) EtcdClient(_ context.Context) (*etcd.Client, error) {
	if d.etcdClient == nil {
		return nil, errors.New("some etcd client error")
	}
	return d.etcdClient, nil
}

func TestLocker_WithoutEtcd(t *testing.T) {
	t.Parallel()

	// Create locker
	d := &testDeps{logger: log.NewDebugLogger()}
	ttlSeconds := 5
	locker := NewLocker(d, ttlSeconds)

	// Test!
	// All attempts return true
	locked1, unlockFn1 := locker.TryLock(context.Background(), "projectId=123")
	defer unlockFn1()
	assert.True(t, locked1)

	locked2, unlockFn2 := locker.TryLock(context.Background(), "projectId=123")
	defer unlockFn2()
	assert.True(t, locked2)

	locked3, unlockFn3 := locker.TryLock(context.Background(), "projectId=123")
	defer unlockFn3()
	assert.True(t, locked3)

	// Check warnings
	expected := `
WARN  cannot acquire etcd lock "projectId=123" (continues without lock): cannot get etcd client: some etcd client error
WARN  cannot acquire etcd lock "projectId=123" (continues without lock): cannot get etcd client: some etcd client error
WARN  cannot acquire etcd lock "projectId=123" (continues without lock): cannot get etcd client: some etcd client error
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), d.logger.AllMessages())
}

func TestLocker_WithEtcd(t *testing.T) {
	t.Parallel()

	// Create locker
	d := &testDeps{logger: log.NewDebugLogger(), etcdClient: etcdhelper.ClientForTest(t)}
	ttlSeconds := 5
	locker := NewLocker(d, ttlSeconds)

	// Test!
	// Project is locked
	locked1, unlock1Fn := locker.TryLock(context.Background(), "projectId=123")
	defer unlock1Fn()
	assert.True(t, locked1)
	// ... so the project cannot be used by other requests
	locked2, unlock2Fn := locker.TryLock(context.Background(), "projectId=123")
	defer unlock2Fn()
	assert.False(t, locked2)
	// ... but another project can be locked
	locked3, unlock3Fn := locker.TryLock(context.Background(), "projectId=789")
	defer unlock3Fn()
	assert.True(t, locked3)
	// Project is unlocked
	unlock1Fn()
	// ... so next request can use the  project
	locked5, unlock5Fn := locker.TryLock(context.Background(), "projectId=123")
	defer unlock5Fn()
	assert.True(t, locked5)
	// Unlock both projects
	unlock3Fn()
	unlock5Fn()

	// Check logged messages
	expected := `
INFO  acquired etcd lock "projectId=123/%s"
INFO  etcd lock "projectId=123" is used
INFO  acquired etcd lock "projectId=789/%s"
INFO  released etcd lock "projectId=123"
INFO  acquired etcd lock "projectId=123/%s"
INFO  released etcd lock "projectId=789"
INFO  released etcd lock "projectId=123"
`
	wildcards.Assert(t, strings.TrimLeft(expected, "\n"), d.logger.AllMessages())
}

func TestLocker_WithEtcd_TimeToLiveExpired(t *testing.T) {
	t.Parallel()

	// Create locker
	d := &testDeps{logger: log.NewDebugLogger(), etcdClient: etcdhelper.ClientForTest(t)}
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
