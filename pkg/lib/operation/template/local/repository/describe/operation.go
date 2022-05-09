package describe

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
)

type dependencies interface {
	Logger() log.Logger
}

func Run(tmpl *template.Template, d dependencies) (err error) {
	logger := d.Logger()

	logger.Infof("Template ID:          %s", tmpl.TemplateRecord().Id)
	logger.Infof("Name:                 %s", tmpl.TemplateRecord().Name)
	logger.Infof("Description:          %s", tmpl.TemplateRecord().Description)
	logger.Infof("")

	v := tmpl.VersionRecord()
	logger.Infof("Version:              %s", v.Version.String())
	logger.Infof("Stable:               %t", v.Stable)
	logger.Infof("Description:          %s", v.Description)
	logger.Infof("")

	// Groups
	for _, group := range tmpl.Inputs().ToExtended() {
		logger.Infof("Group ID:             %s", group.Id)
		logger.Infof("Description:          %s", group.Description)
		logger.Infof("Required:             %s", string(group.Required))
		logger.Infof("")

		// Steps
		for _, step := range group.Steps {
			logger.Infof("  Step ID:            %s", step.Id)
			logger.Infof("  Name:               %s", step.Name)
			logger.Infof("  Description:        %s", step.Description)
			logger.Infof("  Dialog Name:        %s", step.NameFoDialog())
			logger.Infof("  Dialog Description: %s", step.DescriptionForDialog())
			logger.Infof("")

			// Inputs
			for _, in := range step.Inputs {
				logger.Infof("    Input ID:         %s", in.Id)
				logger.Infof("    Name:             %s", in.Name)
				logger.Infof("    Description:      %s", in.Description)
				logger.Infof("    Type:             %s", in.Type)
				logger.Infof("    Kind:             %s", string(in.Kind))
				if in.Default != nil {
					logger.Infof("    Default:          %#v", in.DefaultOrEmpty())
				}
				if len(in.Options) > 0 {
					logger.Infof("    Options:")
					for _, opt := range in.Options {
						logger.Infof("      %s:  %s", opt.Value, opt.Label)
					}
				}
				logger.Infof("")
			}
		}
	}

	return nil
}
