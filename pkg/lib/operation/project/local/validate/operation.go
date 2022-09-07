package validate

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Options struct {
	ValidateSecrets    bool
	ValidateJsonSchema bool
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.validate")
	defer telemetry.EndSpan(span, &err)

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
