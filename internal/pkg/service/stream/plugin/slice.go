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
	stats statistics.Value,
) error

func (p *Plugins) RegisterSliceUploader(provider stagingModel.FileProvider, fn uploadSliceFn) {
	p.sliceUploader[provider] = fn
}

func (p *Plugins) UploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice *model.Slice,
	stats statistics.Value,
) error {
	if _, ok := p.sliceUploader[slice.StagingStorage.Provider]; !ok {
		err := fmt.Sprintf("missing uploadSlice definition for given provider: %v", slice.StagingStorage.Provider)
		return errors.New(err)
	}

	err := p.sliceUploader[slice.StagingStorage.Provider](ctx, volume, slice, stats)
	if err != nil {
		return err
	}

	return err
}
