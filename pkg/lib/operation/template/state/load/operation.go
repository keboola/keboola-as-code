package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	loadManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/load"
)

type Options struct {
	Template     model.TemplateReference
	LoadOptions  loadState.OptionsWithFilter
	JsonNetCtx   *jsonnet.Context
	Replacements replacekeys.Keys
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
	Template(reference model.TemplateReference) (*template.Template, error)
}

func Run(o Options, d dependencies) (*template.State, error) {
	// Get template
	tmpl, err := d.Template(o.Template)
	if err != nil {
		return nil, err
	}

	// Load manifest
	manifest, err := loadManifest.Run(tmpl.Fs(), o.JsonNetCtx, d)
	if err != nil {
		return nil, err
	}

	// Run operation
	evaluated := tmpl.ToEvaluated(manifest, o.JsonNetCtx, o.Replacements, d)
	if state, err := loadState.Run(evaluated, o.LoadOptions, d); err == nil {
		return template.NewState(state, evaluated), nil
	} else {
		return nil, err
	}
}
