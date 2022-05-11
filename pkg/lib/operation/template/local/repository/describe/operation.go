package describe

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(tmpl *template.Template, d dependencies) (err error) {
	logger := d.Logger().InfoWriter()

	logger.Writef("Template ID:          %s", tmpl.TemplateRecord().Id)
	logger.Writef("Name:                 %s", tmpl.TemplateRecord().Name)
	logger.Writef("Description:          %s", tmpl.TemplateRecord().Description)
	logger.Writef("")

	v := tmpl.VersionRecord()
	logger.Writef("Version:              %s", v.Version.String())
	logger.Writef("Stable:               %t", v.Stable)
	logger.Writef("Description:          %s", v.Description)
	logger.Writef("")

	// Groups
	for _, group := range tmpl.Inputs().ToExtended() {
		logger.Writef("Group ID:             %s", group.Id)
		logger.Writef("Description:          %s", group.Description)
		logger.Writef("Required:             %s", string(group.Required))
		logger.Writef("")

		// Steps
		for _, step := range group.Steps {
			logger.Writef("  Step ID:            %s", step.Id)
			logger.Writef("  Name:               %s", step.Name)
			logger.Writef("  Description:        %s", step.Description)
			logger.Writef("  Dialog Name:        %s", step.NameFoDialog())
			logger.Writef("  Dialog Description: %s", step.DescriptionForDialog())
			logger.Writef("")

			// Inputs
			for _, in := range step.Inputs {
				logger.Writef("    Input ID:         %s", in.Id)
				logger.Writef("    Name:             %s", in.Name)
				logger.Writef("    Description:      %s", in.Description)
				logger.Writef("    Type:             %s", in.Type)
				logger.Writef("    Kind:             %s", string(in.Kind))
				if in.Default != nil {
					logger.Writef("    Default:          %#v", in.DefaultOrEmpty())
				}
				if len(in.Options) > 0 {
					logger.Writef("    Options:")
					for _, opt := range in.Options {
						logger.Writef("      %s:  %s", opt.Value, opt.Label)
					}
				}
				logger.Writef("")
			}
		}
	}

	return nil
}
