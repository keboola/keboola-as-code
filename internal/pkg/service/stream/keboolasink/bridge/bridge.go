package bridge

import (
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

const (
	// stagingFileProvider marks staging files provided by the Keboola Storage API.
	stagingFileProvider = staging.FileProvider("keboola")
	// targetProvider marks files which destionation is a Keboola table.
	targetProvider = target.Provider("keboola")
)

type Bridge struct {
	logger      log.Logger
	client      etcd.KV
	schema      schema.Schema
	plugins     *plugin.Plugins
	publicAPI   *keboola.PublicAPI
	apiProvider apiProvider

	getBucketOnce    *singleflight.Group
	createBucketOnce *singleflight.Group
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
	KeboolaPublicAPI() *keboola.PublicAPI
}

func New(d dependencies, apiProvider apiProvider) *Bridge {
	b := &Bridge{
		logger:           d.Logger().WithComponent("keboola.bridge"),
		client:           d.EtcdClient(),
		schema:           schema.New(d.EtcdSerde()),
		plugins:          d.Plugins(),
		publicAPI:        d.KeboolaPublicAPI(),
		apiProvider:      apiProvider,
		getBucketOnce:    &singleflight.Group{},
		createBucketOnce: &singleflight.Group{},
	}

	b.plugins.RegisterSinkWithLocalStorage(func(sink *definition.Sink) bool {
		return sink.Type == definition.SinkTypeTable && sink.Table.Type == definition.TableTypeKeboola
	})

	b.setupOnFileOpen()
	b.deleteCredentialsOnFileDelete()
	b.deleteTokenOnSinkDeactivation()

	return b
}

func (b *Bridge) isKeboolaTableSink(sink *definition.Sink) bool {
	return sink.Type == definition.SinkTypeTable && sink.Table.Type == definition.TableTypeKeboola
}

func (b *Bridge) isKeboolaStagingFile(file *model.File) bool {
	return file.StagingStorage.Provider == stagingFileProvider
}
