package validate

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Options struct {
	ValidateSecrets    bool
	ValidateJSONSchema bool
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.validate")
	defer span.End(&err)

	logger := d.Logger()

	// Validate schemas
	errs := errors.NewMultiError()
	if o.ValidateJSONSchema {
		if err := schema.ValidateObjects(d.Logger(), projectState); err != nil {
			errs.Append(err)
		}
	}

	// Validate all values encrypted
	if o.ValidateSecrets {
		plan := encrypt.NewPlan(projectState)
		if err := plan.ValidateAllEncrypted(); err != nil {
			errs.Append(err)
		}
	}

	// Process errors
	if err := errs.ErrorOrNil(); err != nil {
		return errors.PrefixError(err, "validation failed")
	}

	logger.Debug("Validation done.")
	return nil
}
