package config

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
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	BranchId    storageapi.BranchID
	ComponentId storageapi.ComponentID
	Name        string
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.create.config")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Storage API
	storageApiClient := d.StorageApiClient()

	// Config key
	key := model.ConfigKey{
		BranchId:    o.BranchId,
		ComponentId: o.ComponentId,
	}

	// Generate unique ID
	ticketProvider := storageapi.NewTicketProvider(ctx, storageApiClient)
	ticketProvider.Request(func(ticket *storageapi.Ticket) {
		key.Id = storageapi.ConfigID(ticket.ID)
	})
	if err := ticketProvider.Resolve(); err != nil {
		return fmt.Errorf(`cannot generate new ID: %w`, err)
	}

	// Create and save object
	uow := projectState.LocalManager().NewUnitOfWork(ctx)
	uow.CreateObject(key, o.Name)
	if err := uow.Invoke(); err != nil {
		return fmt.Errorf(`cannot create config: %w`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
