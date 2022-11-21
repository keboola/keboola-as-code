package dependencies

import (
	"context"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// remote dependencies container implements Project interface.
type remote struct {
	dependencies.Project
	ForLocalCommand
}

func newProjectDeps(ctx context.Context, cmdPublicDeps ForLocalCommand) (*remote, error) {
	// Get Storage API token
	token := cmdPublicDeps.Options().GetString(options.StorageApiTokenOpt)
	if token == "" {
		return nil, ErrMissingStorageApiToken
	}

	// Create common remote dependencies (includes API authentication)
	projectDeps, err := dependencies.NewProjectDeps(ctx, cmdPublicDeps, cmdPublicDeps, token)
	if err != nil {
		var storageApiErr *storageapi.Error
		if errors.As(err, &storageApiErr) && storageApiErr.ErrCode == "storage.tokenInvalid" {
			return nil, ErrInvalidStorageApiToken
		}
		return nil, err
	}

	// Storage Api token remote ID and manifest remote ID must be same
	if prj, exists, err := cmdPublicDeps.LocalProject(false); exists && err == nil {
		tokenProjectId := projectDeps.ProjectID()
		manifest := prj.ProjectManifest()
		if manifest != nil && manifest.ProjectID() != tokenProjectId {
			return nil, errors.Errorf(`given token is from the remote "%d", but in manifest is defined remote "%d"`, tokenProjectId, manifest.ProjectID())
		}
	}

	// Compose all together
	return &remote{
		ForLocalCommand: cmdPublicDeps,
		Project:         projectDeps,
	}, nil
}

func storageApiHost(fs filesystem.Fs, opts *options.Options) (string, error) {
	var host string
	if fs.IsFile(projectManifest.Path()) {
		// Get host from remote manifest
		m, err := projectManifest.Load(fs, true)
		if err != nil {
			return "", err
		} else {
			host = m.ApiHost()
		}
	} else {
		// Get host from options (ENV/flag)
		host = opts.GetString(options.StorageApiHostOpt)
	}

	// Fallback
	if host == "" {
		host = "connection.keboola.com"
	}

	// Validate host
	if host = strhelper.NormalizeHost(host); host == "" {
		return "", ErrMissingStorageApiHost
	} else {
		return host, nil
	}
}
