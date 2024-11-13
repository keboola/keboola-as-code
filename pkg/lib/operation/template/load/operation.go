package load

import (
	"context"
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
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

type DeprecatedTemplateError struct {
	templateID string
}

func (e DeprecatedTemplateError) Error() string {
	return fmt.Sprintf(`template "%s" is deprecated, cannot be used`, e.templateID)
}

func (e DeprecatedTemplateError) ErrorName() string {
	return "template.deprecated"
}

func (e DeprecatedTemplateError) ErrorUserMessage() string {
	return fmt.Sprintf(`Template "%s" is deprecated, cannot be used.`, e.templateID)
}

func (e DeprecatedTemplateError) StatusCode() int {
	return http.StatusBadRequest
}

func Run(ctx context.Context, d dependencies, repository *repository.Repository, reference model.TemplateRef, o template.Option) (tmpl *template.Template, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.load")
	defer span.End(&err)

	// Get template
	templateRecord, err := repository.RecordByIDOrErr(reference.TemplateID())
	if err != nil {
		return nil, err
	}

	// Is deprecated?
	if templateRecord.Deprecated {
		return nil, DeprecatedTemplateError{templateID: templateRecord.ID}
	}

	// Get template version
	versionRecord, err := templateRecord.GetVersionOrErr(reference.Version())
	if err != nil {
		return nil, err
	}

	// Check if template dir exists
	templatePath := filesystem.Join(templateRecord.Path, versionRecord.Path)
	if !repository.Fs().IsDir(ctx, templatePath) {
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
	return template.New(ctx, reference, templateRecord, versionRecord, templateDir, repository.CommonDir(), d.Components(), o)
}
