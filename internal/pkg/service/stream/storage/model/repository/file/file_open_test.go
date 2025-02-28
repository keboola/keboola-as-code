package file_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestFileRepository_OpenFileOnSinkActivation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey1 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-1"}
	sinkKey2 := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink-2"}

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, commonDeps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create sinks, it triggers files creation
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(ctx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(ctx).Err())
		sink1 := dummy.NewSinkWithLocalStorage(sinkKey1)
		require.NoError(t, defRepo.Sink().Create(&sink1, clk.Now(), by, "Create sink").Do(ctx).Err())
		sink2 := dummy.NewSinkWithLocalStorage(sinkKey2)
		require.NoError(t, defRepo.Sink().Create(&sink2, clk.Now(), by, "Create sink").Do(ctx).Err())
	}

	// Disable Source, it in cascade disables Sinks, it triggers files closing
	// -----------------------------------------------------------------------------------------------------------------
	{
		clk.Advance(time.Hour)
		require.NoError(t, defRepo.Source().Disable(sourceKey, clk.Now(), by, "test reason").Do(ctx).Err())
	}

	// Enable Source, it in cascade enables Sinks, it triggers files opening
	// -----------------------------------------------------------------------------------------------------------------
	var openEtcdLogs string
	{
		clk.Advance(time.Hour)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Source().Enable(sourceKey, clk.Now(), by).Do(ctx).Err())
		openEtcdLogs = etcdLogs.String()
	}

	// Check etcd operations
	// -----------------------------------------------------------------------------------------------------------------
	etcdlogger.AssertFromFile(t, `fixtures/file_open_ops_001.txt`, openEtcdLogs)

	// Check etcd state
	// -----------------------------------------------------------------------------------------------------------------
	etcdhelper.AssertKVsFromFile(t, client, "fixtures/file_open_snapshot_001.txt", etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/|storage/secret/|storage/volume/|storage/stats/"))
}
