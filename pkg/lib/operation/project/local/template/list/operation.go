package list

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, branch *model.BranchState, d dependencies) (err error) {
	_, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.list")
	defer span.End(&err)

	// Get instances
	instances, err := branch.Local.Metadata.TemplatesInstances()
	if err != nil {
		return err
	}

	w := d.Stdout()

	for _, instance := range instances {
		fmt.Fprintf(w, "Template ID:          %s\n", instance.TemplateID)
		fmt.Fprintf(w, "Instance ID:          %s\n", instance.InstanceID)
		fmt.Fprintf(w, "RepositoryName:       %s\n", instance.RepositoryName)
		fmt.Fprintf(w, "Version:              %s\n", instance.Version)
		fmt.Fprintf(w, "Name:                 %s\n", instance.InstanceName)
		fmt.Fprintln(w, "Created:")
		fmt.Fprintf(w, "  Date:               %s\n", instance.Created.Date.Format(time.RFC3339))
		fmt.Fprintf(w, "  TokenID:            %s\n", instance.Created.TokenID)
		fmt.Fprintln(w, "Updated:")
		fmt.Fprintf(w, "  Date:               %s\n", instance.Updated.Date.Format(time.RFC3339))
		fmt.Fprintf(w, "  TokenID:            %s\n", instance.Updated.TokenID)
		fmt.Fprintln(w)
	}

	return nil
}
