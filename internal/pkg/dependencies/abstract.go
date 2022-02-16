package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi/eventsender"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	loadStateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/state/load"
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
	Components() (*model.ComponentsMap, error)
	StorageApi() (*storageapi.Api, error)
	EncryptionApi() (*encryptionapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
	EventSender() (*eventsender.Sender, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
	Template(reference model.TemplateRef) (*template.Template, error)
	TemplateState(options loadStateOp.Options) (*template.State, error)
	TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error)
	EmptyDir() (filesystem.Fs, error)
	LocalProject() (*project.Project, error)
	LocalProjectExists() bool
	LocalTemplate() (*template.Template, error)
	LocalTemplateExists() bool
	LocalTemplateRepository() (*repository.Repository, error)
	LocalTemplateRepositoryExists() bool
}
