package plugin

import (
	"context"
	"errors"
	"fmt"

	"gocloud.dev/blob"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

const (
	keboolaFileProvider = stagingModel.FileProvider("keboola")
)

type uploadSliceFn func(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	stats statistics.Value,
) (*blob.Writer, diskreader.Reader, error)

func (p *Plugins) ImporterFor(provider stagingModel.FileProvider) error {
	if _, ok := p.sliceUploader[provider]; !ok {
		err := fmt.Sprintf("no importer for given provider: %v", provider)
		return errors.New(err)
	}

	return nil
}

func (p *Plugins) RegisterSliceUploader(provider stagingModel.FileProvider, fn uploadSliceFn) {
	p.sliceUploader[provider] = fn
}

func (p *Plugins) UploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	stats statistics.Value,
) error {
	var err error
	uploader, reader, err := p.sliceUploader[slice.StagingStorage.Provider](ctx, volume, slice, stats)
	if err != nil {
		return err
	}

	defer func() {
		err = reader.Close(ctx)
	}()

	_, err = reader.WriteTo(uploader)
	if err != nil {
		return err
	}

	/*if errors.As(err, &pipeline.NoOpenerFoundError{}) {
		continue
	}*/

	return err
}
