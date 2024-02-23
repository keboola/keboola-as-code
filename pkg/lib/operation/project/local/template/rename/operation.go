package delete_template

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	Branch   model.BranchKey
	Instance model.TemplateInstance
	NewName  string
	TokenID  string
}

type dependencies interface {
	Logger() log.Logger
	StorageAPITokenID() string
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.rename")
	defer span.End(&err)

	logger := d.Logger()

	// Get branch
	branchState := projectState.MustGet(o.Branch).(*model.BranchState)

	// Rename
	o.Instance.InstanceName = o.NewName
	err = branchState.Local.Metadata.UpsertTemplateInstanceFrom(time.Now(), d.StorageAPITokenID(), o.Instance)
	if err != nil {
		return err
	}

	// Get manager
	manager := projectState.LocalManager()

	// Save metadata
	uow := manager.NewUnitOfWork(ctx)
	uow.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())
	if err := uow.Invoke(); err != nil {
		return err
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(ctx, `Rename done.`)
	return nil
}
