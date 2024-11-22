package distlock_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestProvider_NewMutex(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d := dependencies.NewMocked(t, ctx, dependencies.WithEnabledEtcdClient())
	client := d.TestEtcdClient()

	p, err := distlock.NewProvider(ctx, distlock.NewConfig(), d)
	require.NoError(t, err)

	mtx := p.NewMutex("foo/bar")
	etcdhelper.AssertKVsString(t, client, ``)

	require.NoError(t, mtx.Lock(ctx))
	require.ErrorAs(t, mtx.TryLock(ctx), &etcdop.AlreadyLockedError{})
	etcdhelper.AssertKVsString(t, client, `
<<<<<
lock/foo/bar/%s (lease)
-----
%A
>>>>>
`)

	require.NoError(t, mtx.Unlock(ctx))
	etcdhelper.AssertKVsString(t, client, ``)
}
