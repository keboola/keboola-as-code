package plugin

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Slice contains a small subset of actual slice fields that the plugin needs.
type Slice struct {
	model.SliceKey
	LocalStorage        localModel.Slice
	StagingStorage      stagingModel.Slice
	EncodingCompression compression.Config
}

type uploadSliceFn func(
	ctx context.Context,
	volume *diskreader.Volume,
	slice Slice,
	stats statistics.Value,
) error

func (p *Plugins) RegisterSliceUploader(provider stagingModel.FileProvider, fn uploadSliceFn) {
	p.sliceUploader[provider] = fn
}

func (p *Plugins) UploadSlice(
	ctx context.Context,
	volume *diskreader.Volume,
	slice Slice,
	stats statistics.Value,
) error {
	if _, ok := p.sliceUploader[slice.StagingStorage.Provider]; !ok {
		return errors.New(fmt.Sprintf("missing uploadSlice definition for given provider: %v", slice.StagingStorage.Provider))
	}

	return p.sliceUploader[slice.StagingStorage.Provider](ctx, volume, slice, stats)
}
