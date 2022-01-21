package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	createProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

// Container contains dependencies for all use-cases.
type Container interface {
	AbstractDeps
	CommonDeps
}

// NewContainer returns dependencies container for production.
func NewContainer(d AbstractDeps, ctx context.Context) Container {
	return newCommonDeps(d, ctx)
}

// NewTestContainer returns dependencies container for tests. It can be modified.
func NewTestContainer(d AbstractDeps, ctx context.Context) *TestContainer {
	return &TestContainer{common: newCommonDeps(d, ctx)}
}

// AbstractDeps dependencies which obtained in different ways according to use-case.
type AbstractDeps interface {
	Logger() log.Logger
	Fs() filesystem.Fs
	Envs() *env.Map
	ApiVerboseLogs() bool
	StorageApiHost() (string, error)
	StorageApiToken() (string, error)
}

// CommonDeps contains common dependencies for all use-cases.
// It is implemented by dependencies.common struct.
type CommonDeps interface {
	Ctx() context.Context
	EmptyDir() (filesystem.Fs, error)
	StorageApi() (*remote.StorageApi, error)
	EncryptionApi() (*encryption.Api, error)
	SchedulerApi() (*scheduler.Api, error)
	EventSender() (*event.Sender, error)
	Project() (*project.Project, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifestExists() bool
	ProjectManifest() (*project.Manifest, error)
	CreateProjectManifest(o createProjectManifest.Options) (*project.Manifest, error)
	Template() (*template.Template, error)
	TemplateState(loadOptions loadState.OptionsWithFilter) (*template.State, error)
	TemplateDir() (filesystem.Fs, error)
	TemplateManifestExists() bool
	TemplateManifest() (*template.Manifest, error)
	TemplateInputs() (*template.Inputs, error)
	CreateTemplateDir(path string) (filesystem.Fs, error)
	CreateTemplateInputs() (*template.Inputs, error)
	CreateTemplateManifest() (*template.Manifest, error)
	TemplateRepository() (*repository.Repository, error)
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifestExists() bool
	TemplateRepositoryManifest() (*repository.Manifest, error)
	CreateTemplateRepositoryManifest() (*repository.Manifest, error)
}
