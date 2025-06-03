package row

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ulid"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	BranchID    keboola.BranchID
	ComponentID keboola.ComponentID
	ConfigID    keboola.ConfigID
	Name        string
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.create.row")
	defer span.End(&err)

	logger := d.Logger()

	// Config row key
	key := model.ConfigRowKey{
		BranchID:    o.BranchID,
		ComponentID: o.ComponentID,
		ConfigID:    o.ConfigID,
	}

	// Generate unique ID
	newID := ulid.NewDefaultGenerator().NewULID()
	key.ID = keboola.RowID(newID)

	// Create and save object
	uow := projectState.LocalManager().NewUnitOfWork(ctx)
	uow.CreateObject(key, o.Name)
	if err := uow.Invoke(); err != nil {
		return errors.Errorf(`cannot create row: %w`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Infof(ctx, `Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path())
	return nil
}
