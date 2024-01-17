package validateschema

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	SchemaPath string
	FilePath   string
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.validate.schema")
	defer span.End(&err)
	logger := d.Logger()

	// Read schema
	s, err := d.Fs().FileLoader().ReadRawFile(ctx, filesystem.NewFileDef(o.SchemaPath))
	if err != nil {
		return err
	}

	// Read file
	fs := d.Fs()
	f, err := d.Fs().FileLoader().ReadJSONFile(ctx, filesystem.NewFileDef(filesystem.Join(fs.WorkingDir(), o.FilePath)))
	if err != nil {
		return err
	}

	// Validate
	if err := schema.ValidateContent([]byte(s.Content), f.Content); err != nil {
		return err
	}

	logger.Info(ctx, "Validation done.")
	return nil
}
