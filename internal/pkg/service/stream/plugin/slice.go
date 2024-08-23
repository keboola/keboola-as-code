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

type ReaderClosureError struct {
	SliceKey model.SliceKey
}

func (e ReaderClosureError) Error() string {
	return fmt.Sprintf("closing of reader was not possible for slice :%q", e.SliceKey)
}

type SendSliceUploadEventError struct {
	SliceKey model.SliceKey
}

func (e SendSliceUploadEventError) Error() string {
	return fmt.Sprintf("send of event was not possible for slice :%q", e.SliceKey)
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
	if _, ok := p.sliceUploader[slice.StagingStorage.Provider]; !ok {
		err := fmt.Sprintf("missing uploadSlice definition for given provider: %v", slice.StagingStorage.Provider)
		return errors.New(err)
	}

	err := p.sliceUploader[slice.StagingStorage.Provider](ctx, volume, slice, stats)
	if !errors.As(err, &ReaderClosureError{}) && !errors.As(err, &SendSliceUploadEventError{}) {
		return err
	}

	// Unsuccessful closing of reader will switch the state of slice to Uploaded
	return nil
}
