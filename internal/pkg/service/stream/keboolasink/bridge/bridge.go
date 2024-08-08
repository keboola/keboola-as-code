package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"
	"gocloud.dev/blob"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	// stagingFileProvider marks staging files provided by the Keboola Storage API.
	stagingFileProvider = stagingModel.FileProvider("keboola")
	// targetProvider marks files which destionation is a Keboola table.
	targetProvider = targetModel.Provider("keboola")

	// Upload slice timeout
	uploadEventSendTimeout = 30 * time.Second
	// sinkMetaKey is a key of the table metadata that marks each table created by the stream.sink.
	sinkMetaKey = "KBC.stream.sink.id"
	// sourceMetaKey is a key of the table metadata that marks each table created by the stream.source.
	sourceMetaKey = "KBC.stream.source.id"
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

func New(d dependencies, fileProvider stagingModel.FileProvider, apiProvider apiProvider) *Bridge {
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

	b.setupOnFileOpen()
	b.deleteCredentialsOnFileDelete()
	b.deleteTokenOnSinkDeactivation()
	b.plugins.RegisterSliceUploader(
		fileProvider,
		func(ctx context.Context, volume *diskreader.Volume, slice *model.Slice, stats statistics.Value) (*blob.Writer, diskreader.Reader, error) {
			var err error
			reader, err := volume.OpenReader(slice)
			if err != nil {
				b.logger.Warnf(ctx, "unable to open reader: %v", err)
				return nil, nil, err
			}

			credentials := b.schema.UploadCredentials().ForFile(slice.FileKey).GetOrEmpty(b.client).Do(ctx).Result()
			// token := b.schema.Token().ForSink(slice.SinkKey).GetOrEmpty(b.client).Do(ctx).Result()
			defer func() {
				ctx, cancel := context.WithTimeout(ctx, uploadEventSendTimeout)
				// TODO: time.Now
				b.sendSliceUploadEvent(ctx, nil, 0, slice, stats)
				cancel()
			}()

			fmt.Println(credentials)
			uploader, err := keboola.NewUploadSliceWriter(ctx, &credentials.FileUploadCredentials, slice.String())
			if err != nil {
				return nil, reader, err
			}

			// Compress to GZip and measure count/size
			/*gzipWr, err := gzip.NewWriterLevel(uploader, slice.Encoding.Compression.GZIP.Level)
			if err != nil {
				return err
			}*/

			return uploader, reader, err
		})

	return b
}

func (b *Bridge) isKeboolaTableSink(sink *definition.Sink) bool {
	return sink.Type == definition.SinkTypeTable && sink.Table.Type == definition.TableTypeKeboola
}

func (b *Bridge) isKeboolaStagingFile(file *model.File) bool {
	return file.StagingStorage.Provider == stagingFileProvider
}
