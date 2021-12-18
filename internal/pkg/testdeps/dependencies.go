package testdeps

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Dependencies struct {
	CtxValue                context.Context
	EnvsValue               *env.Map
	FsValue                 filesystem.Fs
	LoggerValue             log.Logger
	OptionsValue            *options.Options
	StorageApiValue         *remote.StorageApi
	EncryptionApiValue      *encryption.Api
	SchedulerApiValue       *scheduler.Api
	EventSenderValue        *event.Sender
	ProjectManifestValue    *projectManifest.Manifest
	RepositoryManifestValue *repositoryManifest.Manifest
	StateValue              *state.State
}

func NewDependencies() *Dependencies {
	d := &Dependencies{}
	d.CtxValue = context.Background()
	d.EnvsValue = env.Empty()
	d.FsValue = testhelper.NewMemoryFs()
	d.OptionsValue = options.New()
	return d
}

func (c Dependencies) Ctx() context.Context {
	if c.CtxValue == nil {
		panic(fmt.Errorf(`"ctx" is not set in testing dependencies`))
	}
	return c.CtxValue
}

func (c Dependencies) Envs() *env.Map {
	if c.EnvsValue == nil {
		panic(fmt.Errorf(`"envs" is not set in testing dependencies`))
	}
	return c.EnvsValue
}

func (c Dependencies) BasePath() string {
	if c.FsValue == nil {
		panic(fmt.Errorf(`"fs" is not set in testing dependencies`))
	}
	return c.FsValue.BasePath()
}

func (c Dependencies) EmptyDir() (filesystem.Fs, error) {
	if c.FsValue == nil {
		panic(fmt.Errorf(`"fs" is not set in testing dependencies`))
	}
	return c.FsValue, nil
}

func (c Dependencies) ProjectDir() (filesystem.Fs, error) {
	if c.FsValue == nil {
		panic(fmt.Errorf(`"fs" is not set in testing dependencies`))
	}
	return c.FsValue, nil
}

func (c Dependencies) Logger() log.Logger {
	if c.LoggerValue == nil {
		panic(fmt.Errorf(`"logger" is not set in testing dependencies`))
	}
	return c.LoggerValue
}

func (c Dependencies) Options() *options.Options {
	if c.OptionsValue == nil {
		panic(fmt.Errorf(`"options" is not set in testing dependencies`))
	}
	return c.OptionsValue
}

func (c Dependencies) SetStorageApiHost(host string) {
	if c.OptionsValue == nil {
		panic(fmt.Errorf(`"options" is not set in testing dependencies`))
	}
	c.OptionsValue.Set(`storage-api-host`, host)
}

func (c Dependencies) SetStorageApiToken(host string) {
	if c.OptionsValue == nil {
		panic(fmt.Errorf(`"options" is not set in testing dependencies`))
	}
	c.OptionsValue.Set(`storage-api-token`, host)
}

func (c Dependencies) StorageApi() (*remote.StorageApi, error) {
	if c.StorageApiValue == nil {
		panic(fmt.Errorf(`"storageApi" is not set in testing dependencies`))
	}
	return c.StorageApiValue, nil
}

func (c Dependencies) EncryptionApi() (*encryption.Api, error) {
	if c.EncryptionApiValue == nil {
		panic(fmt.Errorf(`"encryptionApi" is not set in testing dependencies`))
	}
	return c.EncryptionApiValue, nil
}

func (c Dependencies) SchedulerApi() (*scheduler.Api, error) {
	if c.SchedulerApiValue == nil {
		panic(fmt.Errorf(`"schedulerApi" is not set in testing dependencies`))
	}
	return c.SchedulerApiValue, nil
}

func (c Dependencies) EventSender() (*event.Sender, error) {
	if c.EventSenderValue == nil {
		panic(fmt.Errorf(`"eventSender" is not set in testing dependencies`))
	}
	return c.EventSenderValue, nil
}

func (c Dependencies) ProjectManifest() (*projectManifest.Manifest, error) {
	if c.ProjectManifestValue == nil {
		panic(fmt.Errorf(`"project manifest" is not set in testing dependencies`))
	}
	return c.ProjectManifestValue, nil
}

func (c Dependencies) RepositoryManifest() (*repositoryManifest.Manifest, error) {
	if c.RepositoryManifestValue == nil {
		panic(fmt.Errorf(`"repository manifest" is not set in testing dependencies`))
	}
	return c.RepositoryManifestValue, nil
}

func (c Dependencies) LoadStateOnce(_ loadState.Options) (*state.State, error) {
	if c.StateValue == nil {
		panic(fmt.Errorf(`"state" is not set in testing dependencies`))
	}
	return c.StateValue, nil
}
