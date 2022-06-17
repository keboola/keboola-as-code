package delete_template

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	Branch   model.BranchKey
	Instance model.TemplateInstance
	NewName  string
	TokenId  string
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApiClient() (client.Sender, error)
}

func Run(projectState *project.State, o Options, d dependencies) error {
	logger := d.Logger()

	// Get Storage Api - for token
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Get branch
	branchState := projectState.MustGet(o.Branch).(*model.BranchState)

	// Rename
	o.Instance.InstanceName = o.NewName
	err = branchState.Local.Metadata.UpsertTemplateInstanceFrom(time.Now(), storageApi.Token().Id, o.Instance)
	if err != nil {
		return err
	}

	// Save metadata
	uow := projectState.LocalManager().NewUnitOfWork(d.Ctx())
	uow.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())
	if err := uow.Invoke(); err != nil {
		return err
	}

	// Save manifest
	if _, err := saveManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(`Rename done.`)
	return nil
}
