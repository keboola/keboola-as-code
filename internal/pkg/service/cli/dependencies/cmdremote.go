package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// remoteCommandScope dependencies container implements RemoteCommandScope interface.
type remoteCommandScope struct {
	dependencies.ProjectScope
	LocalCommandScope
	eventSender event.Sender
}

func newRemoteCommandScope(ctx context.Context, localCmdScp LocalCommandScope, opts ...Option) (*remoteCommandScope, error) {
	cfg := newConfig(opts)

	// Get Storage API token
	token, err := storageAPIToken(localCmdScp)
	if err != nil {
		return nil, err
	}

	// Create common remote dependencies (includes API authentication)
	var projectOps []dependencies.ProjectScopeOption
	if cfg.withoutMasterToken {
		projectOps = append(projectOps, dependencies.WithoutMasterToken())
	}
	prjScp, err := dependencies.NewProjectDeps(ctx, localCmdScp, token, projectOps...)
	if err != nil {
		var storageAPIErr *keboola.StorageError
		if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
			return nil, ErrInvalidStorageAPIToken
		}
		return nil, err
	}

	// Storage Api token project ID and manifest remote ID must be same
	if prj, exists, err := localCmdScp.LocalProject(ctx, false); exists && err == nil {
		tokenProjectID := prjScp.ProjectID()
		manifest := prj.ProjectManifest()
		if manifest != nil && manifest.ProjectID() != tokenProjectID {
			return nil, errors.Errorf(`given token is from the project "%d", but in manifest is defined project "%d"`, tokenProjectID, manifest.ProjectID())
		}
	}

	eventSender := event.NewSender(localCmdScp.Logger(), prjScp.KeboolaProjectAPI(), prjScp.ProjectID())

	// Compose all together
	return &remoteCommandScope{
		LocalCommandScope: localCmdScp,
		ProjectScope:      prjScp,
		eventSender:       eventSender,
	}, nil
}

func (r *remoteCommandScope) EventSender() event.Sender {
	return r.eventSender
}
