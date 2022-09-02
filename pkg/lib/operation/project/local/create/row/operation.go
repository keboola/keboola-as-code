package row

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	BranchId    storageapi.BranchID
	ComponentId storageapi.ComponentID
	ConfigId    storageapi.ConfigID
	Name        string
}

type dependencies interface {
	Logger() log.Logger
	StorageApiClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	logger := d.Logger()

	// Get Storage API
	storageApiClient := d.StorageApiClient()

	// Config row key
	key := model.ConfigRowKey{
		BranchId:    o.BranchId,
		ComponentId: o.ComponentId,
		ConfigId:    o.ConfigId,
	}

	// Generate unique ID
	ticketProvider := storageapi.NewTicketProvider(ctx, storageApiClient)
	ticketProvider.Request(func(ticket *storageapi.Ticket) {
		key.Id = storageapi.RowID(ticket.ID)
	})
	if err := ticketProvider.Resolve(); err != nil {
		return fmt.Errorf(`cannot generate new ID: %w`, err)
	}

	// Create and save object
	uow := projectState.LocalManager().NewUnitOfWork(ctx)
	uow.CreateObject(key, o.Name)
	if err := uow.Invoke(); err != nil {
		return fmt.Errorf(`cannot create row: %w`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
