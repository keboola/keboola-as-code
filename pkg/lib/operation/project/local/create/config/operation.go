package config

import (
	"context"
	"math/rand"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/oklog/ulid/v2"

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
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.create.config")
	defer span.End(&err)

	logger := d.Logger()

	// Config key
	key := model.ConfigKey{
		BranchID:    o.BranchID,
		ComponentID: o.ComponentID,
	}

	// Generate unique ID
	ms := ulid.Timestamp(time.Now())
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	newID := ulid.MustNew(ms, entropy).String()
	key.ID = keboola.ConfigID(newID)

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

	logger.Infof(ctx, `Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path())
	return nil
}
