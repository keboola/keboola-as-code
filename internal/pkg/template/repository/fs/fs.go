package fs

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type config struct {
	onlyForTemplate model.TemplateRef
}

type Option func(config *config)

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Logger() log.Logger
}

func OnlyForTemplate(ref model.TemplateRef) Option {
	return func(config *config) {
		config.onlyForTemplate = ref
	}
}

func For(ctx context.Context, d dependencies, ref model.TemplateRepository, opts ...Option) (fs filesystem.Fs, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.declarative.templates.repository.fs.For")
	defer telemetry.EndSpan(span, &err)

	switch ref.Type {
	case model.RepositoryTypeDir:
		return aferofs.NewLocalFs(ref.URL, filesystem.WithLogger(d.Logger()))
	case model.RepositoryTypeGit:
		return gitFsFor(ctx, d, ref, opts...)
	default:
		panic(errors.Errorf(`unexpected repository type "%s"`, ref.Type))
	}
}
