// Package dependencies provides dependencies for command line interface.
//
// # Dependency Containers
//
// This package extends common dependencies from [pkg/github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies].
//
// Following dependencies containers are implemented:
//   - [BaseScope] interface provides basic CLI dependencies.
//   - [LocalCommandScope] interface provides dependencies for commands that do not modify the remote project
//   - [RemoteCommandScope] interface provides dependencies for commands that modify the remote project.
//
// These containers can be obtained from the [Provider], it can be created by [NewProvider].
package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/event"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

var (
	ErrMissingStorageAPIHost      = errors.New(`missing Storage API host`)
	ErrMissingStorageAPIToken     = errors.New(`missing Storage API token`)
	ErrInvalidStorageAPIToken     = errors.New(`invalid Storage API token`)
	ErrProjectManifestNotFound    = errors.New("local manifest not found")
	ErrDbtProjectNotFound         = errors.Errorf(`dbt project not found, missing file "%s"`, dbt.ProjectFilePath)
	ErrTemplateManifestNotFound   = errors.New("template manifest not found")
	ErrRepositoryManifestNotFound = errors.New("repository manifest not found")
)

// BaseScope interface provides basic CLI dependencies.
type BaseScope interface {
	dependencies.BaseScope
	CommandCtx() context.Context
	Fs() filesystem.Fs
	FsInfo() FsInfo
	Dialogs() *dialog.Dialogs
	Options() *options.Options
	EmptyDir() (filesystem.Fs, error)
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
}

// LocalCommandScope interface provides dependencies for commands that do not modify the remote project.
// It contains CLI dependencies that are available from the Storage API and other sources without authentication / Storage API token.
type LocalCommandScope interface {
	BaseScope
	dependencies.PublicScope
	Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error)
	LocalProject(ignoreErrors bool) (*projectPkg.Project, bool, error)
	LocalTemplate(ctx context.Context) (*template.Template, bool, error)
	LocalTemplateRepository(ctx context.Context) (*repository.Repository, bool, error)
}

// RemoteCommandScope interface provides dependencies for commands that modify remote project.
// It contains CLI dependencies that require authentication / Storage API token.
type RemoteCommandScope interface {
	LocalCommandScope
	dependencies.ProjectScope
	EventSender() event.Sender
}

// Provider of CLI dependencies.
type Provider interface {
	BaseScope() BaseScope
	LocalCommandScope(opts ...Option) (LocalCommandScope, error)
	RemoteCommandScope(opts ...Option) (RemoteCommandScope, error)
	// LocalProject method can be used by a CLI command that must be run in the local project directory.
	// First, the local project is loaded, and then the authentication is performed,
	// so the error that we are not in a project directory takes precedence over an invalid/missing token.
	LocalProject(ignoreErrors bool, ops ...Option) (*projectPkg.Project, RemoteCommandScope, error)
	// LocalRepository method can be used by a CLI command that must be run in the local repository directory.
	LocalRepository(ops ...Option) (*repository.Repository, LocalCommandScope, error)
	// LocalDbtProject method can be used by a CLI command that must be run in the dbt project directory.
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
}
