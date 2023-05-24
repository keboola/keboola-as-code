package describe

import (
	"context"

	markdown "github.com/MichaelMure/go-term-markdown"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, tmpl *template.Template, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.repository.describe")
	defer telemetry.EndSpan(span, &err)

	w := d.Logger().InfoWriter()

	w.Writef("Template ID:          %s", tmpl.TemplateRecord().ID)
	w.Writef("Name:                 %s", tmpl.TemplateRecord().Name)
	w.Writef("Description:          %s", tmpl.TemplateRecord().Description)
	w.Writef("")

	if len(tmpl.LongDesc()) > 0 {
		w.Writef("Extended Description:")
		w.Writef(string(markdown.Render(tmpl.LongDesc(), 80, 2)))
		w.Writef("")
	}

	v := tmpl.VersionRecord()
	w.Writef("Version:              %s", v.Version.String())
	w.Writef("Stable:               %t", v.Stable)
	w.Writef("Description:          %s", v.Description)
	if len(v.Components) > 0 {
		w.Writef("Components:")
		for _, c := range v.Components {
			w.Writef("  - %s", c)
		}
	}
	w.Writef("")

	// Groups
	for _, group := range tmpl.Inputs().ToExtended() {
		w.Writef("Group ID:             %s", group.ID)
		w.Writef("Description:          %s", group.Description)
		w.Writef("Required:             %s", string(group.Required))
		w.Writef("")

		// Steps
		for _, step := range group.Steps {
			w.Writef("  Step ID:            %s", step.ID)
			w.Writef("  Name:               %s", step.Name)
			w.Writef("  Description:        %s", step.Description)
			w.Writef("  Dialog Name:        %s", step.NameForDialog())
			w.Writef("  Dialog Description: %s", step.DescriptionForDialog())
			w.Writef("")

			// Inputs
			for _, in := range step.Inputs {
				w.Writef("    Input ID:         %s", in.ID)
				w.Writef("    Name:             %s", in.Name)
				w.Writef("    Description:      %s", in.Description)
				w.Writef("    Type:             %s", in.Type)
				w.Writef("    Kind:             %s", string(in.Kind))
				if in.Default != nil {
					w.Writef("    Default:          %#v", in.DefaultOrEmpty())
				}
				if len(in.Options) > 0 {
					w.Writef("    Options:")
					for _, opt := range in.Options {
						w.Writef("      %s:  %s", opt.Value, opt.Label)
					}
				}
				w.Writef("")
			}
		}
	}

	return nil
}
