package bucket

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	Description string
	DisplayName string
	Name        string
	Stage       string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.create.bucket")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	if !strings.HasPrefix(o.Name, "c-") {
		o.Name = "c-" + o.Name
	}
	b := &keboola.Bucket{
		ID: keboola.BucketID{
			Stage:      o.Stage,
			BucketName: o.Name,
		},
		Description: o.Description,
		DisplayName: o.DisplayName,
	}
	b, err = d.KeboolaProjectAPI().CreateBucketRequest(b).Send(ctx)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf(`Created bucket "%s".`, b.ID.String()))
	return nil
}
