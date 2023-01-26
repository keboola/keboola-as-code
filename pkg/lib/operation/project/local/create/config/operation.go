package config

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
)

type Options struct {
	BranchID    keboola.BranchID
	ComponentID keboola.ComponentID
	Name        string
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	KeboolaAPIClient() client.Sender
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.create.config")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Storage API
	storageAPIClient := d.KeboolaAPIClient()

	// Config key
	key := model.ConfigKey{
		BranchID:    o.BranchID,
		ComponentID: o.ComponentID,
	}

	// Generate unique ID
	ticketProvider := keboola.NewTicketProvider(ctx, storageAPIClient)
	ticketProvider.Request(func(ticket *keboola.Ticket) {
		key.ID = keboola.ConfigID(ticket.ID)
	})
	if err := ticketProvider.Resolve(); err != nil {
		return errors.Errorf(`cannot generate new ID: %w`, err)
	}

	// Create and save object
	uow := projectState.LocalManager().NewUnitOfWork(ctx)
	uow.CreateObject(key, o.Name)
	if err := uow.Invoke(); err != nil {
		return errors.Errorf(`cannot create config: %w`, err)
	}

	// Save manifest
	if _, err := saveManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
