package plugin

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type uploadSliceFn func(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	alreadyUploadedSlices map[model.FileKey]string,
	stats statistics.Value,
) error

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
	alreadyUploadedSlices map[model.FileKey]string,
	stats statistics.Value,
) error {
	err := p.sliceUploader[slice.StagingStorage.Provider](ctx, volume, slice, alreadyUploadedSlices, stats)
	if err != nil {
		return err
	}

	/*if errors.As(err, &pipeline.NoOpenerFoundError{}) {
		continue
	}*/

	return err
}
