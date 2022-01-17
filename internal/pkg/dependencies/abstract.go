package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/project/state/load"
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
	Project() (*project.Project, error)
	Template() (*template.Template, error)
	TemplateRepository() (*repository.Repository, error)
	ApiVerboseLogs() bool
	StorageApiHost() (string, error)
	StorageApiToken() (string, error)
}

// CommonDeps contains common dependencies for all use-cases.
// It is implemented by dependencies.common struct.
type CommonDeps interface {
	Ctx() context.Context
	StorageApi() (*remote.StorageApi, error)
	EncryptionApi() (*encryption.Api, error)
	SchedulerApi() (*scheduler.Api, error)
	EventSender() (*event.Sender, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
	TemplateState(loadOptions loadState.Options) (*template.State, error)
}
