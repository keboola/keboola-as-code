package validate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	ValidateSecrets    bool
	ValidateJsonSchema bool
}

type dependencies interface {
	Logger() log.Logger
	LoadStateOnce(loadOptions loadState.Options) (*state.State, error)
}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(o Options, d dependencies) (err error) {
	logger := d.Logger()
	projectState, err := d.LoadStateOnce(LoadStateOptions())
	if err != nil {
		return err
	}

	errors := utils.NewMultiError()

	// Validate schemas
	if o.ValidateJsonSchema {
		if err := schema.ValidateSchemas(projectState.State); err != nil {
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
