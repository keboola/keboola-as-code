package row

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	BranchId    model.BranchId
	ComponentId model.ComponentId
	ConfigId    model.ConfigId
	Name        string
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*manifest.Manifest, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
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

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Load state
	projectState, err := d.ProjectState(LoadStateOptions())
	if err != nil {
		return err
	}

	// Config row key
	key := model.ConfigRowKey{
		BranchId:    o.BranchId,
		ComponentId: o.ComponentId,
		ConfigId:    o.ConfigId,
	}

	// Generate unique ID
	ticketProvider := storageApi.NewTicketProvider()
	ticketProvider.Request(func(ticket *model.Ticket) {
		key.Id = model.RowId(ticket.Id)
	})
	if err := ticketProvider.Resolve(); err != nil {
		return fmt.Errorf(`cannot generate new ID: %w`, err)
	}

	// Create and save object
	uow := projectState.LocalManager().NewUnitOfWork(d.Ctx())
	uow.CreateObject(key, o.Name)
	if err := uow.Invoke(); err != nil {
		return fmt.Errorf(`cannot create row: %w`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(d); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
