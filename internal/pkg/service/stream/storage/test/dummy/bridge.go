package dummy

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	fileProviderType = stagingModel.FileProvider("test")
)

type Bridge struct{}

func NewBridge() *Bridge {
	return &Bridge{}
}

func (b *Bridge) RegisterDummyImporter(plugins *plugin.Plugins) {
	// Register dummy sink with local storage support for tests
	plugins.RegisterSliceUploader(
		fileProviderType,
		func(ctx context.Context, volume *diskreader.Volume, slice *model.Slice, uploadedSlices map[model.FileKey]string, stats statistics.Value) error {
			reader, err := volume.OpenReader(slice)
			if err != nil {
				return err
			}

			defer func() {
				err = reader.Close(ctx)
			}()

			return nil
		})
}
