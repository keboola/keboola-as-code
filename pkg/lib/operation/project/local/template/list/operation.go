package list

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, branch *model.BranchState, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.list")
	defer telemetry.EndSpan(span, &err)

	w := d.Logger().InfoWriter()

	// Get instances
	instances, err := branch.Local.Metadata.TemplatesInstances()
	if err != nil {
		return err
	}

	for _, instance := range instances {
		w.Writef("Template ID:          %s", instance.TemplateID)
		w.Writef("Instance ID:          %s", instance.InstanceID)
		w.Writef("RepositoryName:       %s", instance.RepositoryName)
		w.Writef("Version:              %s", instance.Version)
		w.Writef("Name:                 %s", instance.InstanceName)
		w.Writef("Created:")
		w.Writef("  Date:               %s", instance.Created.Date.Format(time.RFC3339))
		w.Writef("  TokenID:            %s", instance.Created.TokenID)
		w.Writef("Updated:")
		w.Writef("  Date:               %s", instance.Updated.Date.Format(time.RFC3339))
		w.Writef("  TokenID:            %s", instance.Updated.TokenID)
		w.Writef("")
	}

	return nil
}
