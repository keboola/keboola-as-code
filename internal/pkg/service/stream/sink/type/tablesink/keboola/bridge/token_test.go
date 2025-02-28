package bridge_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/encryption"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/schema"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/test"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestBridge_MigrateTokens(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	ignoredKeys := etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/all/|storage/slice/all/|storage/volume/")
	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	// Get services
	d, mocked := dependencies.NewMockedAPIScopeWithConfig(t, ctx, func(cfg *config.Config) {
		// Lock configuration, so it is not affected by the default values
		cfg.Storage.Level.Staging.Upload.Trigger = staging.UploadTrigger{
			Count:    10000,
			Size:     1 * datasize.MB,
			Interval: duration.From(1 * time.Minute),
		}
		cfg.Storage.Level.Target.Import.Trigger = target.ImportTrigger{
			Count:       50000,
			Size:        5 * datasize.MB,
			Interval:    duration.From(5 * time.Minute),
			SlicesCount: 100,
			Expiration:  duration.From(30 * time.Minute),
		}
		cfg.Encryption.Provider = encryption.ProviderAES
		cfg.Encryption.AES = &encryption.AESConfig{
			SecretKey: secretKey,
		}
	}, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// apiCtx - for operations triggered by an authorized API call
	apiCtx := rollback.ContextWith(ctx, rollback.New(d.Logger()))
	apiCtx = context.WithValue(apiCtx, dependencies.KeboolaProjectAPICtxKey, mocked.KeboolaProjectAPI())

	// Register mocked responses
	// -----------------------------------------------------------------------------------------------------------------
	transport := mocked.MockedHTTPTransport()
	{
		bridgeTest.MockTokenStorageAPICalls(t, transport)
		bridgeTest.MockBucketStorageAPICalls(t, transport)
		bridgeTest.MockTableStorageAPICalls(t, transport)
		bridgeTest.MockSuccessJobStorageAPICalls(t, transport)
		bridgeTest.MockFileStorageAPICalls(t, clk, transport)
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(apiCtx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(apiCtx).Err())
	}

	// Create sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var createSinkEtcdLogs string
	{
		sink := test.NewKeboolaTableSink(sinkKey)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(apiCtx).Err())
		createSinkEtcdLogs = etcdLogs.String()
	}
	{
		// Storage API calls
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets/in.c-bucket/tables-definition"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/tables/in.c-bucket.my-table/metadata"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"created bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"created table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"creating staging file","token.ID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1001","file.resourceID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/create_sink_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/create_sink_ops.txt", createSinkEtcdLogs)
	}

	// Create tokens
	// -----------------------------------------------------------------------------------------------------------------
	bridgeSchema := schema.New(d.EtcdSerde())

	sinkKey1 := sinkKey
	sinkKey1.SinkID = "nonexistent-sink-1"
	{
		newToken := keboolasink.Token{
			SinkKey: sinkKey1,
			Token: &keboola.Token{
				Token: "token1",
				ID:    "1",
			},
		}
		err := op.
			Atomic(client, &op.NoResult{}).
			AddFrom(op.Atomic(d.EtcdClient(), &newToken).
				Write(func(ctx context.Context) op.Op {
					return bridgeSchema.Token().ForSink(newToken.SinkKey).Put(d.EtcdClient(), newToken)
				}),
			).Do(ctx).Err()
		require.NoError(t, err)
	}

	sinkKey2 := sinkKey
	sinkKey2.SinkID = "nonexistent-sink-2"
	{
		newToken := keboolasink.Token{
			SinkKey: sinkKey2,
			Token: &keboola.Token{
				Token: "token2",
				ID:    "2",
			},
		}
		err := op.
			Atomic(client, &op.NoResult{}).
			AddFrom(op.Atomic(d.EtcdClient(), &newToken).
				Write(func(ctx context.Context) op.Op {
					return bridgeSchema.Token().ForSink(newToken.SinkKey).Put(d.EtcdClient(), newToken)
				}),
			).Do(ctx).Err()
		require.NoError(t, err)
	}

	// Migrate tokens
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := d.KeboolaSinkBridge().MigrateTokens(ctx)
		require.NoError(t, err)
	}

	// Verify that tokens are encrypted
	// -----------------------------------------------------------------------------------------------------------------
	encryptor := cloudencrypt.NewGenericEncryptor[keboola.Token](d.Encryptor())

	var token1 keboolasink.Token
	{
		err := bridgeSchema.Token().ForSink(sinkKey1).GetOrErr(client).WithResultTo(&token1).Do(ctx).Err()
		require.NoError(t, err)
		assert.NotNil(t, token1.Token)
		assert.Equal(t, "1", token1.TokenID)
		assert.NotNil(t, token1.EncryptedToken)

		metadata := cloudencrypt.Metadata{"sink": token1.SinkKey.String()}
		keboolaToken, err := encryptor.Decrypt(ctx, []byte(token1.EncryptedToken), metadata)
		require.NoError(t, err)
		assert.Equal(t, "token1", keboolaToken.Token)
	}
	var token2 keboolasink.Token
	{
		err := bridgeSchema.Token().ForSink(sinkKey2).GetOrErr(client).WithResultTo(&token2).Do(ctx).Err()
		require.NoError(t, err)
		assert.NotNil(t, token2.Token)
		assert.Equal(t, "2", token2.TokenID)
		assert.NotNil(t, token2.EncryptedToken)

		metadata := cloudencrypt.Metadata{"sink": token2.SinkKey.String()}
		keboolaToken, err := encryptor.Decrypt(ctx, []byte(token2.EncryptedToken), metadata)
		require.NoError(t, err)
		assert.Equal(t, "token2", keboolaToken.Token)
	}

	// Check that second migration has no impact
	// -----------------------------------------------------------------------------------------------------------------
	{
		err := d.KeboolaSinkBridge().MigrateTokens(ctx)
		require.NoError(t, err)
	}
	{
		var token keboolasink.Token
		err := bridgeSchema.Token().ForSink(sinkKey1).GetOrErr(client).WithResultTo(&token).Do(ctx).Err()
		require.NoError(t, err)
		assert.NotNil(t, token.Token)
		assert.Equal(t, "1", token.TokenID)
		assert.Equal(t, token.EncryptedToken, token1.EncryptedToken)
	}
	{
		var token keboolasink.Token
		err := bridgeSchema.Token().ForSink(sinkKey2).GetOrErr(client).WithResultTo(&token).Do(ctx).Err()
		require.NoError(t, err)
		assert.NotNil(t, token.Token)
		assert.Equal(t, "2", token.TokenID)
		assert.Equal(t, token.EncryptedToken, token2.EncryptedToken)
	}
}

func TestBridge_EncryptDecryptTokens(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-01T01:00:00.000Z").Time())
	by := test.ByUser()

	// Fixtures
	projectID := keboola.ProjectID(123)
	branchKey := key.BranchKey{ProjectID: projectID, BranchID: 456}
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	sinkKey := key.SinkKey{SourceKey: sourceKey, SinkID: "my-sink"}
	ignoredKeys := etcdhelper.WithIgnoredKeyPattern("^definition/|storage/file/|storage/slice/|storage/volume/")
	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	// Get services
	d, mocked := dependencies.NewMockedAPIScopeWithConfig(t, ctx, func(cfg *config.Config) {
		// Lock configuration, so it is not affected by the default values
		cfg.Storage.Level.Staging.Upload.Trigger = staging.UploadTrigger{
			Count:    10000,
			Size:     1 * datasize.MB,
			Interval: duration.From(1 * time.Minute),
		}
		cfg.Storage.Level.Target.Import.Trigger = target.ImportTrigger{
			Count:       50000,
			Size:        5 * datasize.MB,
			Interval:    duration.From(5 * time.Minute),
			SlicesCount: 100,
			Expiration:  duration.From(30 * time.Minute),
		}
		cfg.Encryption.Provider = encryption.ProviderAES
		cfg.Encryption.AES = &encryption.AESConfig{
			SecretKey:      secretKey,
			NonceGenerator: func(int) ([]byte, error) { return []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, nil },
		}
	}, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	defRepo := d.DefinitionRepository()
	storageRepo := d.StorageRepository()
	volumeRepo := storageRepo.Volume()

	// Log etcd operations
	var etcdLogs bytes.Buffer
	rawClient := d.EtcdClient()
	rawClient.KV = etcdlogger.KVLogWrapper(rawClient.KV, &etcdLogs, etcdlogger.WithMinimal())

	// apiCtx - for operations triggered by an authorized API call
	apiCtx := rollback.ContextWith(ctx, rollback.New(d.Logger()))
	apiCtx = context.WithValue(apiCtx, dependencies.KeboolaProjectAPICtxKey, mocked.KeboolaProjectAPI())

	// Register mocked responses
	// -----------------------------------------------------------------------------------------------------------------
	transport := mocked.MockedHTTPTransport()
	{
		bridgeTest.MockTokenStorageAPICalls(t, transport)
		bridgeTest.MockBucketStorageAPICalls(t, transport)
		bridgeTest.MockTableStorageAPICalls(t, transport)
		bridgeTest.MockSuccessJobStorageAPICalls(t, transport)
		bridgeTest.MockFileStorageAPICalls(t, clk, transport)
	}

	// Register active volumes
	// -----------------------------------------------------------------------------------------------------------------
	{
		session, err := concurrency.NewSession(client)
		require.NoError(t, err)
		defer func() { require.NoError(t, session.Close()) }()
		test.RegisterWriterVolumes(t, ctx, volumeRepo, session, 1)
	}

	// Create parent branch, source, sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	{
		branch := test.NewBranch(branchKey)
		require.NoError(t, defRepo.Branch().Create(&branch, clk.Now(), by).Do(apiCtx).Err())
		source := test.NewSource(sourceKey)
		require.NoError(t, defRepo.Source().Create(&source, clk.Now(), by, "Create source").Do(apiCtx).Err())
	}

	// Create sink, file, slice
	// -----------------------------------------------------------------------------------------------------------------
	var createSinkEtcdLogs string
	{
		sink := test.NewKeboolaTableSink(sinkKey)
		etcdLogs.Reset()
		require.NoError(t, defRepo.Sink().Create(&sink, clk.Now(), by, "Create sink").Do(apiCtx).Err())
		createSinkEtcdLogs = etcdLogs.String()
	}
	{
		// Storage API calls
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/tokens"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/buckets/in.c-bucket/tables-definition"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/tables/in.c-bucket.my-table/metadata"])
		assert.Equal(t, 1, transport.GetCallCountInfo()["POST https://connection.keboola.local/v2/storage/branch/456/files/prepare"])
		transport.ZeroCallCounters()
	}
	{
		// Logs
		mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"created bucket","bucket.key":"456/in.c-bucket"}
{"level":"info","message":"creating token","token.bucketID":"in.c-bucket"}
{"level":"info","message":"created token","token.bucketID":"in.c-bucket","token.name":"[_internal] Stream Sink my-source/my-sink"}
{"level":"info","message":"creating table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"created table","table.key":"456/in.c-bucket.my-table"}
{"level":"info","message":"creating staging file","token.ID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
{"level":"info","message":"created staging file","token.ID":"1001","file.resourceID":"1001","file.name":"my-source_my-sink_20000101010000","file.id":"2000-01-01T01:00:00.000Z"}
`)
		mocked.DebugLogger().Truncate()
	}
	{
		// Etcd state
		etcdhelper.AssertKVsFromFile(t, client, "fixtures/encrypt_sink_snapshot.txt", ignoredKeys)
	}
	{
		// Etcd operations
		etcdlogger.AssertFromFile(t, "fixtures/encrypt_sink_ops.txt", createSinkEtcdLogs)
	}

	// Verify that tokens are encrypted
	// -----------------------------------------------------------------------------------------------------------------
	bridgeSchema := schema.New(d.EtcdSerde())
	encryptor := cloudencrypt.NewGenericEncryptor[keboola.Token](d.Encryptor())
	var token keboolasink.Token
	{
		err := bridgeSchema.Token().ForSink(sinkKey).GetOrErr(client).WithResultTo(&token).Do(ctx).Err()
		require.NoError(t, err)
		assert.Nil(t, token.Token)
		assert.Equal(t, "1001", token.TokenID)
		assert.NotNil(t, token.EncryptedToken)

		metadata := cloudencrypt.Metadata{"sink": token.SinkKey.String()}
		keboolaToken, err := encryptor.Decrypt(ctx, []byte(token.EncryptedToken), metadata)
		require.NoError(t, err)
		assert.Equal(t, "my-token", keboolaToken.Token)
	}
}
