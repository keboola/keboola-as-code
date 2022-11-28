package row

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	BranchID    storageapi.BranchID
	ComponentID storageapi.ComponentID
	ConfigID    storageapi.ConfigID
	Name        string
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageAPIClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.create.row")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Storage API
	storageAPIClient := d.StorageAPIClient()

	// Config row key
	key := model.ConfigRowKey{
		BranchID:    o.BranchID,
		ComponentID: o.ComponentID,
		ConfigID:    o.ConfigID,
	}

	// Generate unique ID
	ticketProvider := storageapi.NewTicketProvider(ctx, storageAPIClient)
	ticketProvider.Request(func(ticket *storageapi.Ticket) {
		key.ID = storageapi.RowID(ticket.ID)
	})
	if err := ticketProvider.Resolve(); err != nil {
		return errors.Errorf(`cannot generate new ID: %w`, err)
	}

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

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
