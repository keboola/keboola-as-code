package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Components() *model.ComponentsMap
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, d dependencies, repository *repository.Repository, reference model.TemplateRef) (tmpl *template.Template, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.load")
	defer telemetry.EndSpan(span, &err)

	// Get template
	templateRecord, err := repository.RecordByIDOrErr(reference.TemplateID())
	if err != nil {
		return nil, err
	}

	// Get template version
	versionRecord, err := templateRecord.GetVersionOrErr(reference.Version())
	if err != nil {
		return nil, err
	}

	// Check if template dir exists
	templatePath := versionRecord.Path()
	if !repository.Fs().IsDir(templatePath) {
		return nil, errors.Errorf(`template dir "%s" not found`, templatePath)
	}

	// Template dir
	templateDir, err := repository.Fs().SubDirFs(templatePath)
	if err != nil {
		return nil, err
	}

	// Update sem version in reference
	reference = model.NewTemplateRef(reference.Repository(), reference.TemplateID(), versionRecord.Version.String())

	// Load template
	return template.New(ctx, reference, templateRecord, versionRecord, templateDir, repository.CommonDir(), d.Components())
}
