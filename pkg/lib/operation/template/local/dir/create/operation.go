package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	Path string
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, repositoryDir filesystem.Fs, o Options, d dependencies) (fs filesystem.Fs, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.dir.create")
	defer telemetry.EndSpan(span, &err)

	// Create template dir
	if err := repositoryDir.Mkdir(o.Path); err != nil {
		return nil, err
	}
	d.Logger().Infof(`Created template dir "%s".`, o.Path)

	// Return FS for the template dir
	return repositoryDir.SubDirFs(o.Path)
}
