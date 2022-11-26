package encrypt

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	DryRun   bool
	LogEmpty bool
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	ProjectID() int
	EncryptionAPIClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.encrypt")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Encryption API
	encryptionAPIClient := d.EncryptionAPIClient()

	// Get plan
	plan := encrypt.NewPlan(projectState)

	// Log plan
	if !plan.Empty() || o.LogEmpty {
		plan.Log(logger)
	}

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info("Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(ctx, d.ProjectID(), logger, encryptionAPIClient, projectState.State()); err != nil {
			return err
		}

		d.Logger().Info("Encrypt done.")
	}

	return nil
}
