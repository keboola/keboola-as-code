package list

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, repo *repository.Repository, d dependencies) (err error) {
	_, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.list")
	defer span.End(&err)

	w := d.Stdout()

	for _, tmpl := range repo.Templates() {
		fmt.Fprintf(w, "Template ID:          %s\n", tmpl.ID)
		fmt.Fprintf(w, "Name:                 %s\n", tmpl.Name)
		fmt.Fprintf(w, "Description:          %s\n", tmpl.Description)
		v, found := tmpl.DefaultVersion()
		if found {
			fmt.Fprintf(w, "Default version:      %s\n", v.Version.String())
		}
		fmt.Fprintln(w)

		for _, v := range tmpl.AllVersions() {
			fmt.Fprintf(w, "  Version:            %s\n", v.Version.String())
			fmt.Fprintf(w, "  Stable:             %t\n", v.Stable)
			fmt.Fprintf(w, "  Description:        %s\n", v.Description)
			fmt.Fprintln(w)
		}

		fmt.Fprintln(w)
	}

	return nil
}
