package bridge

import (
	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/schema"
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
	logger            log.Logger
	config            keboolasink.Config
	client            etcd.KV
	schema            schema.Schema
	plugins           *plugin.Plugins
	publicAPI         *keboola.PublicAPI
	apiProvider       apiProvider
	storageRepository *storageRepo.Repository
	clock             clock.Clock

	getBucketOnce    *singleflight.Group
	createBucketOnce *singleflight.Group
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	KeboolaPublicAPI() *keboola.PublicAPI
	Clock() clock.Clock
	StorageRepository() *storageRepo.Repository
}

func New(d dependencies, apiProvider apiProvider, config keboolasink.Config) *Bridge {
	b := &Bridge{
		logger:            d.Logger().WithComponent("keboola.bridge"),
		config:            config,
		client:            d.EtcdClient(),
		schema:            schema.New(d.EtcdSerde()),
		plugins:           d.Plugins(),
		publicAPI:         d.KeboolaPublicAPI(),
		apiProvider:       apiProvider,
		storageRepository: d.StorageRepository(),
		clock:             d.Clock(),
		getBucketOnce:     &singleflight.Group{},
		createBucketOnce:  &singleflight.Group{},
	}

	b.setupOnFileOpen()
	b.deleteCredentialsOnFileDelete()
	b.deleteTokenOnSinkDeactivation()
	b.plugins.RegisterFileImporter(targetProvider, b.importFile)
	b.plugins.RegisterSliceUploader(stagingFileProvider, b.uploadSlice)

	return b
}

func (b *Bridge) isKeboolaTableSink(sink *definition.Sink) bool {
	return sink.Type == definition.SinkTypeTable && sink.Table.Type == definition.TableTypeKeboola
}

func (b *Bridge) isKeboolaStagingFile(file *model.File) bool {
	return file.StagingStorage.Provider == stagingFileProvider
}
