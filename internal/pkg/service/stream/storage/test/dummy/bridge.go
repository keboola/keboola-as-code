package dummy

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"gocloud.dev/blob"
)

const (
	fileProviderType = stagingModel.FileProvider("test")
)

type Bridge struct {
}

func NewBridge() *Bridge {
	return &Bridge{}
}

func (b *Bridge) RegisterDummyImporter(plugins *plugin.Plugins) {
	// Register dummy sink with local storage support for tests
	plugins.RegisterSliceUploader(
		fileProviderType,
		func(ctx context.Context, volume *diskreader.Volume, slice *model.Slice, stats statistics.Value) (*blob.Writer, diskreader.Reader, error) {
			var err error
			reader, err := volume.OpenReader(slice)
			if err != nil {
				// b.logger.Warnf(ctx, "unable to open reader: %v", err)
				return nil, nil, err
			}

			//credentials := b.schema.UploadCredentials().ForFile(slice.FileKey).GetOrEmpty(b.client).Do(ctx).Result()
			defer func() {
				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_ = ctx
				// TODO: time.Now
				// b.sendSliceUploadEvent(ctx, 0, slice, stats)
				cancel()
			}()

			credentials := keboolasink.FileUploadCredentials{}
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
}
