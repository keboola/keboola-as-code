package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// remote dependencies container implements Project interface.
type remote struct {
	dependencies.Project
	ForLocalCommand
	eventSender event.Sender
}

func newProjectDeps(ctx context.Context, cmdPublicDeps ForLocalCommand) (*remote, error) {
	// Get Storage API token
	token := cmdPublicDeps.Options().GetString(options.StorageAPITokenOpt)
	if token == "" {
		return nil, ErrMissingStorageAPIToken
	}

	// Create common remote dependencies (includes API authentication)
	projectDeps, err := dependencies.NewProjectDeps(ctx, cmdPublicDeps, cmdPublicDeps, token)
	if err != nil {
		var storageAPIErr *storageapi.Error
		if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
			return nil, ErrInvalidStorageAPIToken
		}
		return nil, err
	}

	// Storage Api token remote ID and manifest remote ID must be same
	if prj, exists, err := cmdPublicDeps.LocalProject(false); exists && err == nil {
		tokenProjectID := projectDeps.ProjectID()
		manifest := prj.ProjectManifest()
		if manifest != nil && manifest.ProjectID() != tokenProjectID {
			return nil, errors.Errorf(`given token is from the remote "%d", but in manifest is defined remote "%d"`, tokenProjectID, manifest.ProjectID())
		}
	}

	eventSender := event.NewSender(cmdPublicDeps.Logger(), projectDeps.StorageAPIClient(), projectDeps.ProjectID())

	// Compose all together
	return &remote{
		ForLocalCommand: cmdPublicDeps,
		Project:         projectDeps,
		eventSender:     eventSender,
	}, nil
}

func storageAPIHost(fs filesystem.Fs, opts *options.Options) (string, error) {
	var host string
	if fs.IsFile(projectManifest.Path()) {
		// Get host from remote manifest
		m, err := projectManifest.Load(fs, true)
		if err != nil {
			return "", err
		} else {
			host = m.APIHost()
		}
	} else {
		// Get host from options (ENV/flag)
		host = opts.GetString(options.StorageAPIHostOpt)
	}

	// Fallback
	if host == "" {
		host = "connection.keboola.com"
	}

	// Validate host
	if host = strhelper.NormalizeHost(host); host == "" {
		return "", ErrMissingStorageAPIHost
	} else {
		return host, nil
	}
}

func (r *remote) EventSender() event.Sender {
	return r.eventSender
}
