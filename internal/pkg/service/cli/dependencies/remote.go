package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// remote dependencies container implements Project interface.
type remote struct {
	dependencies.Project
	ForLocalCommand
	eventSender event.Sender
}

func newRemote(ctx context.Context, cmdPublicDeps ForLocalCommand, opts ...Option) (*remote, error) {
	cfg := newConfig(opts)

	// Get Storage API token
	token, err := storageAPIToken(cmdPublicDeps)
	if err != nil {
		return nil, err
	}

	// Create common remote dependencies (includes API authentication)
	var projectOps []dependencies.ProjectDepsOption
	if cfg.withoutMasterToken {
		projectOps = append(projectOps, dependencies.WithoutMasterToken())
	}
	projectDeps, err := dependencies.NewProjectDeps(ctx, cmdPublicDeps, cmdPublicDeps, token, projectOps...)
	if err != nil {
		var storageAPIErr *keboola.StorageError
		if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
			return nil, ErrInvalidStorageAPIToken
		}
		return nil, err
	}

	// Storage Api token project ID and manifest remote ID must be same
	if prj, exists, err := cmdPublicDeps.LocalProject(false); exists && err == nil {
		tokenProjectID := projectDeps.ProjectID()
		manifest := prj.ProjectManifest()
		if manifest != nil && manifest.ProjectID() != tokenProjectID {
			return nil, errors.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, tokenProjectID, manifest.ProjectID())
		}
	}

	eventSender := event.NewSender(cmdPublicDeps.Logger(), projectDeps.KeboolaProjectAPI(), projectDeps.ProjectID())

	// Compose all together
	return &remote{
		ForLocalCommand: cmdPublicDeps,
		Project:         projectDeps,
		eventSender:     eventSender,
	}, nil
}

func (r *remote) EventSender() event.Sender {
	return r.eventSender
}
