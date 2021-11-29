package row

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/save"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	BranchId    int
	ComponentId string
	ConfigId    string
	Name        string
}

type dependencies interface {
	Ctx() context.Context
	Logger() *zap.SugaredLogger
	StorageApi() (*remote.StorageApi, error)
	Manifest() (*manifest.Manifest, error)
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

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Load state
	projectState, err := d.LoadStateOnce(LoadStateOptions())
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
		key.Id = ticket.Id
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
