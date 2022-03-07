package validate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Options struct {
	ValidateSecrets    bool
	ValidateJsonSchema bool
}

type dependencies interface {
	Logger() log.Logger
}

func Run(projectState *project.State, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Validate schemas
	errors := utils.NewMultiError()
	if o.ValidateJsonSchema {
		if err := schema.ValidateSchemas(projectState); err != nil {
			errors.Append(err)
		}
	}

	// Validate all values encrypted
	if o.ValidateSecrets {
		plan := encrypt.NewPlan(projectState)
		if err := plan.ValidateAllEncrypted(); err != nil {
			errors.Append(err)
		}
	}

	// Process errors
	if err := errors.ErrorOrNil(); err != nil {
		return utils.PrefixError("validation failed", err)
	}

	logger.Debug("Validation done.")
	return nil
}
