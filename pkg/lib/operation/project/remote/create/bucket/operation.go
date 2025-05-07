package bucket

import (
	"context"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	BranchKey   keboola.BranchKey
	Description string
	DisplayName string
	Name        string
	Stage       string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.create.bucket")
	defer span.End(&err)

	logger := d.Logger()

	if !strings.HasPrefix(o.Name, "c-") {
		o.Name = "c-" + o.Name
	}
	b := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: o.BranchKey.ID,
			BucketID: keboola.BucketID{
				Stage:      o.Stage,
				BucketName: o.Name,
			},
		},
		Description: o.Description,
		DisplayName: o.DisplayName,
	}
	b, err = d.KeboolaProjectAPI().CreateBucketRequest(b).Send(ctx)
	if err != nil {
		return err
	}
	logger.Infof(ctx, `Created bucket "%s".`, b.BucketID.String())
	return nil
}
