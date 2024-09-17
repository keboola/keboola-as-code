package coordinator_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestStart_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d, mock := dependencies.NewMockedCoordinatorScope(t, ctx)

	// Start
	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentStorageCoordinator))

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Logs
	mock.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"starting storage coordinator node","component":"storage.node.coordinator"} 
{"level":"info","message":"joining distribution group","distribution.group":"operator.file.rotation","distribution.node":"test-node","component":"distribution"}
{"level":"info","message":"joining distribution group","distribution.group":"operator.slice.rotation","distribution.node":"test-node","component":"distribution"}
{"level":"info","message":"joining distribution group","distribution.group":"operator.file.import","distribution.node":"test-node","component":"distribution"}
{"level":"info","message":"joining distribution group","distribution.group":"storage.metadata.cleanup","distribution.node":"test-node","component":"distribution"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"received shutdown request","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd session: context canceled","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"closed etcd session","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"shutdown done","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}
