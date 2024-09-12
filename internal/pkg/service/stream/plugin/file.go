package plugin

import (
	"context"
	"fmt"

	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// File contains a small subset of actual file fields that the plugin needs.
type File struct {
	model.FileKey
	IsEmpty  bool
	Provider targetModel.Provider
}

type importFileFn func(ctx context.Context, file File, stats statistics.Value) error

func (p *Plugins) RegisterFileImporter(provider targetModel.Provider, fn importFileFn) {
	p.fileImport[provider] = fn
}

func (p *Plugins) ImportFile(ctx context.Context, file File, stats statistics.Value) error {
	if _, ok := p.fileImport[file.Provider]; !ok {
		return errors.New(fmt.Sprintf("no importer for given provider: %v", file.Provider))
	}

	return p.fileImport[file.Provider](ctx, file, stats)
}
