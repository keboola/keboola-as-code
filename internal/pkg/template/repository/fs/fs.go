package fs

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type config struct {
	onlyForTemplate model.TemplateRef
}

type Option func(config *config)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func OnlyForTemplate(ref model.TemplateRef) Option {
	return func(config *config) {
		config.onlyForTemplate = ref
	}
}

func For(ctx context.Context, d dependencies, ref model.TemplateRepository, opts ...Option) (fs filesystem.Fs, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.templates.repository.fs.For")
	defer telemetry.EndSpan(span, &err)

	switch ref.Type {
	case model.RepositoryTypeDir:
		return aferofs.NewLocalFs(d.Logger(), ref.Url, "")
	case model.RepositoryTypeGit:
		return gitFsFor(ctx, d, ref, opts...)
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, ref.Type))
	}
}
