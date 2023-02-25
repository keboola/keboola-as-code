package download

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	ProjectFeatures() keboola.FeaturesMap
	Logger() log.Logger
	Tracer() trace.Tracer
}

type Options struct {
	File   *keboola.FileDownloadCredentials
	Output string
}

func Run(ctx context.Context, _ Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.file.download")
	defer telemetry.EndSpan(span, &err)

	return nil
}
