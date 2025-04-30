package upgrade

import (
	"context"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

type Options struct {
	Branch                model.BranchKey
	Instance              model.TemplateInstance
	Inputs                template.InputsValues
	SkipEncrypt           bool
	SkipSecretsValidation bool
}

type dependencies interface {
	Logger() log.Logger
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider
	ProjectID() keboola.ProjectID
	ProjectBackends() []string
	StorageAPIHost() string
	StorageAPITokenID() string
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, tmpl *template.Template, o Options, d dependencies) (result *use.Result, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.upgrade")
	defer span.End(&err)

	// Create tickets provider, to generate new IDs, if needed
	tickets := d.ObjectIDGeneratorFactory()(ctx)

	// Prepare template
	tmplCtx := upgrade.NewContext(ctx, tmpl.Reference(), tmpl.ObjectsRoot(), o.Instance.InstanceID, o.Branch, o.Inputs, tmpl.Inputs().InputsMap(), tickets, d.Components(), projectState.State(), d.ProjectBackends())
	plan, err := use.PrepareTemplate(ctx, d, use.ExtendedOptions{
		TargetBranch:          o.Branch,
		Inputs:                o.Inputs,
		InstanceID:            o.Instance.InstanceID,
		InstanceName:          o.Instance.InstanceName,
		ProjectState:          projectState,
		Template:              tmpl,
		TemplateCtx:           tmplCtx,
		Upgrade:               true,
		SkipEncrypt:           o.SkipEncrypt,
		SkipSecretsValidation: o.SkipSecretsValidation,
	})
	if err != nil {
		return nil, err
	}

	return plan.Invoke(ctx)
}
