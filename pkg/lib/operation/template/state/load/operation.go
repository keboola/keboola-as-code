package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	loadManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/load"
)

type Options struct {
	Template    *template.Template
	Context     template.Context
	LoadOptions loadState.Options
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

func Run(o Options, d dependencies) (*template.State, error) {
	// Load manifest
	manifest, err := loadManifest.Run(o.Template.Fs(), o.Context, d)
	if err != nil {
		return nil, err
	}

	// Run operation
	localFilter := o.Context.LocalObjectsFilter()
	remoteFilter := o.Context.RemoteObjectsFilter()
	loadOptions := loadState.OptionsWithFilter{
		Options:      o.LoadOptions,
		LocalFilter:  &localFilter,
		RemoteFilter: &remoteFilter,
	}
	container := o.Template.ToObjectsContainer(o.Context, manifest, d)
	if state, err := loadState.Run(container, loadOptions, d); err == nil {
		return template.NewState(state, container), nil
	} else {
		return nil, err
	}
}
