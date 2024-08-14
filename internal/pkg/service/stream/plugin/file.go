package plugin

import (
	"context"
	"fmt"

	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Importer func(ctx context.Context, file *model.File) error

func (p *Plugins) RegisterFileImporter(provider targetModel.Provider, fn Importer) {
	p.fileImport[provider] = fn
}

func (p *Plugins) ImportFile(ctx context.Context, file *model.File) error {
	if _, ok := p.fileImport[file.TargetStorage.Provider]; !ok {
		return errors.New(fmt.Sprintf("no importer for given provider: %v", file.TargetStorage.Provider))
	}

	return p.fileImport[file.TargetStorage.Provider](ctx, file)
}
