package bridge

import (
	"context"
	"sync"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	bridgeModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	keboolaBridgeRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model/schema"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
)

const (
	// stagingFileProvider marks staging files provided by the Keboola Storage API.
	stagingFileProvider = stagingModel.FileProvider("keboola")
	// targetProvider marks files which destination is a Keboola table.
	targetProvider = targetModel.Provider("keboola")

	// sinkMetaKey is a key of the table metadata that marks each table created by the stream.sink.
	sinkMetaKey = "KBC.stream.sink.id"
	// sourceMetaKey is a key of the table metadata that marks each table created by the stream.source.
	sourceMetaKey = "KBC.stream.source.id"
)

type Bridge struct {
	logger                  log.Logger
	config                  keboolasink.Config
	client                  etcd.KV
	schema                  schema.Schema
	plugins                 *plugin.Plugins
	publicAPI               *keboola.PublicAPI
	apiProvider             apiProvider
	storageRepository       *storageRepo.Repository
	keboolaBridgeRepository *keboolaBridgeRepo.Repository
	locks                   *distlock.Provider
	jobs                    *etcdop.MirrorMap[bridgeModel.Job, key.JobKey, *jobData]

	getBucketOnce    *singleflight.Group
	createBucketOnce *singleflight.Group
}

type jobData struct {
	key.JobKey
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Process() *servicectx.Process
	Plugins() *plugin.Plugins
	KeboolaPublicAPI() *keboola.PublicAPI
	StorageRepository() *storageRepo.Repository
	KeboolaBridgeRepository() *keboolaBridgeRepo.Repository
	DistributedLockProvider() *distlock.Provider
}

func New(d dependencies, apiProvider apiProvider, config keboolasink.Config) *Bridge {
	b := &Bridge{
		logger:                  d.Logger().WithComponent("keboola.bridge"),
		config:                  config,
		client:                  d.EtcdClient(),
		schema:                  schema.New(d.EtcdSerde()),
		plugins:                 d.Plugins(),
		publicAPI:               d.KeboolaPublicAPI(),
		apiProvider:             apiProvider,
		storageRepository:       d.StorageRepository(),
		keboolaBridgeRepository: d.KeboolaBridgeRepository(),
		locks:                   d.DistributedLockProvider(),
		getBucketOnce:           &singleflight.Group{},
		createBucketOnce:        &singleflight.Group{},
	}

	b.setupOnFileOpen()
	b.deleteCredentialsOnFileDelete()
	b.deleteTokenOnSinkDeactivation()
	b.plugins.RegisterFileImporter(targetProvider, b.importFile)
	b.plugins.RegisterSliceUploader(stagingFileProvider, b.uploadSlice)
	b.plugins.RegisterCanAcceptNewFile(targetProvider, b.canAcceptNewFile)
	return b
}

func (b *Bridge) isKeboolaTableSink(sink *definition.Sink) bool {
	return sink.Type == definition.SinkTypeTable && sink.Table.Type == definition.TableTypeKeboola
}

func (b *Bridge) isKeboolaStagingFile(file *model.File) bool {
	return file.StagingStorage.Provider == stagingFileProvider
}

func (b *Bridge) MirrorJobs(ctx context.Context, d dependencies) error {
	// Mirror jobs
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(ctx)
	d.Process().OnShutdown(func(_ context.Context) {
		b.logger.Info(ctx, "closing bridge job mirror")

		// Stop mirroring
		cancel()
		wg.Wait()

		b.logger.Info(ctx, "closed bridge job mirror")
	})
	b.jobs = etcdop.SetupMirrorMap[bridgeModel.Job, key.JobKey, *jobData](
		b.keboolaBridgeRepository.Job().GetAllAndWatch(ctx, etcd.WithPrevKV()),
		func(_ string, job bridgeModel.Job) key.JobKey {
			return job.JobKey
		},
		func(_ string, job bridgeModel.Job, rawValue *op.KeyValue, oldValue **jobData) *jobData {
			return &jobData{
				job.JobKey,
			}
		},
	).BuildMirror()
	if err := <-b.jobs.StartMirroring(ctx, wg, b.logger); err != nil {
		b.logger.Errorf(ctx, "cannot start mirroring jobs: %s", err)
		return err
	}

	return nil
}
