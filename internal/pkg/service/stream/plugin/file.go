package plugin

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// File contains a small subset of actual file fields that the plugin needs.
type File struct {
	FileKey               model.FileKey
	SinkKey               key.SinkKey
	TargetStorageProvider targetModel.Provider
}

type Importer func(ctx context.Context, file *File) error

func (p *Plugins) RegisterFileImporter(provider targetModel.Provider, fn Importer) {
	p.fileImport[provider] = fn
}

func (p *Plugins) ImportFile(ctx context.Context, file *File) error {
	if _, ok := p.fileImport[file.TargetStorageProvider]; !ok {
		return errors.New(fmt.Sprintf("no importer for given provider: %v", file.TargetStorageProvider))
	}

	return p.fileImport[file.TargetStorageProvider](ctx, file)
}
