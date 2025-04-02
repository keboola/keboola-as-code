package describe

import (
	"context"
	"fmt"
	"io"

	markdown "github.com/MichaelMure/go-term-markdown"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, tmpl *template.Template, d dependencies) (err error) {
	_, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.describe")
	defer span.End(&err)

	w := d.Stdout()

	fmt.Fprintf(w, "Template ID:          %s\n", tmpl.TemplateRecord().ID)
	fmt.Fprintf(w, "Name:                 %s\n", tmpl.TemplateRecord().Name)
	fmt.Fprintf(w, "Description:          %s\n", tmpl.TemplateRecord().Description)
	fmt.Fprintln(w)

	if len(tmpl.LongDesc()) > 0 {
		fmt.Fprintln(w, "Extended Description:")
		fmt.Fprintln(w, string(markdown.Render(tmpl.LongDesc(), 80, 2)))
		fmt.Fprintln(w, "")
	}

	v := tmpl.VersionRecord()
	fmt.Fprintf(w, "Version:              %s\n", v.Version.String())
	fmt.Fprintf(w, "Stable:               %t\n", v.Stable)
	fmt.Fprintf(w, "Description:          %s\n", v.Description)
	if len(v.Components) > 0 {
		fmt.Fprintln(w, "Components:")
		for _, c := range v.Components {
			fmt.Fprintf(w, "  - %s\n", c)
		}
	}
	fmt.Fprintln(w)

	// Groups
	for _, group := range tmpl.Inputs().ToExtended() {
		fmt.Fprintf(w, "Group ID:             %s\n", group.ID)
		fmt.Fprintf(w, "Description:          %s\n", group.Description)
		fmt.Fprintf(w, "Required:             %s\n", string(group.Required))
		fmt.Fprintln(w)

		// Steps
		for _, step := range group.Steps {
			fmt.Fprintf(w, "  Step ID:            %s\n", step.ID)
			fmt.Fprintf(w, "  Name:               %s\n", step.Name)
			fmt.Fprintf(w, "  Description:        %s\n", step.Description)
			fmt.Fprintf(w, "  Dialog Name:        %s\n", step.NameForDialog())
			fmt.Fprintf(w, "  Dialog Description: %s\n", step.DescriptionForDialog())
			fmt.Fprintln(w)

			// Inputs
			for _, in := range step.Inputs {
				fmt.Fprintf(w, "    Input ID:         %s\n", in.ID)
				fmt.Fprintf(w, "    Name:             %s\n", in.Name)
				fmt.Fprintf(w, "    Description:      %s\n", in.Description)
				fmt.Fprintf(w, "    Type:             %s\n", in.Type)
				fmt.Fprintf(w, "    Kind:             %s\n", string(in.Kind))
				if in.Default != nil {
					fmt.Fprintf(w, "    Default:          %#v\n", in.DefaultOrEmpty())
				}
				if len(in.Options) > 0 {
					fmt.Fprintln(w, "    Options:")
					for _, opt := range in.Options {
						fmt.Fprintf(w, "      %s:  %s\n", opt.Value, opt.Label)
					}
				}
				fmt.Fprintln(w)
			}
		}
	}

	return nil
}
