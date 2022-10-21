package upgrade

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	saveProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
)

type Options struct {
	Branch   model.BranchKey
	Instance model.TemplateInstance
	Inputs   template.InputsValues
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	ProjectID() int
	StorageApiHost() string
	StorageApiTokenID() string
	StorageApiClient() client.Sender
	SchedulerApiClient() client.Sender
	Components() *model.ComponentsMap
	EncryptionApiClient() client.Sender
	ObjectIDGeneratorFactory() func(ctx context.Context) *storageapi.TicketProvider
}

func Run(ctx context.Context, projectState *project.State, tmpl *template.Template, o Options, d dependencies) (warnings []string, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.template.upgrade")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()
	storageApiHost := d.StorageApiHost()
	projectID := d.ProjectID()

	// Create tickets provider, to generate new IDs, if needed
	tickets := d.ObjectIDGeneratorFactory()(ctx)

	// Load template
	tmplCtx := upgrade.NewContext(ctx, tmpl.Reference(), tmpl.ObjectsRoot(), o.Instance.InstanceId, o.Branch, o.Inputs, tmpl.Inputs().InputsMap(), tickets, d.Components(), projectState.State())
	templateState, err := tmpl.LoadState(tmplCtx, use.LoadTemplateOptions(), d)
	if err != nil {
		return nil, err
	}

	// Get manager
	manager := projectState.LocalManager()

	// Prepare operations
	objects := make(upgradedObjects, 0)
	errs := errors.NewMultiError()
	renameOp := manager.NewPathsGenerator(true)
	saveOp := manager.NewUnitOfWork(ctx)

	// Store template information in branch metadata
	branchState := projectState.GetOrNil(o.Branch).(*model.BranchState)

	// Get main config
	mainConfig, err := templateState.MainConfig()
	if err != nil {
		return nil, err
	}

	// Update instance metadata
	if err := branchState.Local.Metadata.UpsertTemplateInstance(time.Now(), o.Instance.InstanceId, o.Instance.InstanceName, tmpl.TemplateId(), tmpl.Repository().Name, tmpl.Version(), d.StorageApiTokenID(), mainConfig); err != nil {
		errs.Append(err)
	}
	saveOp.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())

	// Save all objects
	for _, tmplObjectState := range templateState.All() {
		// Skip branch - it is already processed
		if tmplObjectState.Kind().IsBranch() {
			continue
		}

		var opMark string
		var objectState model.ObjectState
		if v, found := projectState.Get(tmplObjectState.Key()); found {
			opMark = diff.ChangeMark
			objectState = v
			objectState.SetLocalState(tmplObjectState.LocalState())

			// Clear path
			objectState.Manifest().SetParentPath("")
			objectState.Manifest().SetRelativePath("")
			renameOp.Add(objectState)
		} else {
			opMark = diff.AddMark
			objectState = tmplObjectState

			// Clear path
			objectState.Manifest().SetParentPath("")
			objectState.Manifest().SetRelativePath("")

			// Copy state from template to project
			if err := projectState.Set(objectState); err != nil {
				errs.Append(err)
				continue
			}

			// Generate path
			renameOp.Add(objectState)
		}

		objects = append(objects, upgradedObject{ObjectState: objectState, opMark: opMark})

		// Save to filesystem
		saveOp.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields())
	}

	// Delete
	var toDelete []model.Key
	configs := search.ConfigsForTemplateInstance(projectState.LocalObjects().ConfigsWithRowsFrom(o.Branch), o.Instance.InstanceId)
	for _, config := range configs {
		if _, found := templateState.Get(config.Key()); !found {
			toDelete = append(toDelete, config.Key())
		}
		for _, row := range config.Rows {
			if _, found := templateState.Get(row.Key()); !found {
				toDelete = append(toDelete, row.Key())
			}
		}
	}
	for _, key := range toDelete {
		objectState := projectState.MustGet(key)
		saveOp.DeleteObject(objectState, objectState.Manifest())
		objects = append(objects, upgradedObject{ObjectState: objectState, opMark: diff.DeleteMark})
	}

	// Execute rename and save
	if errs.Len() > 0 {
		return nil, errs
	}

	if err := renameOp.Invoke(); err != nil {
		return nil, err
	}
	if err := saveOp.Invoke(); err != nil {
		return nil, err
	}

	// Encrypt values
	if err := encrypt.Run(ctx, projectState, encrypt.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return nil, err
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return nil, err
	}

	// Log new objects
	objects.Log(logger, tmpl)

	// Normalize paths
	if _, err := rename.Run(ctx, projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return nil, err
	}

	// Validate schemas and encryption
	if err := validate.Run(ctx, projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	// Return urls to oauth configurations
	warnings = make([]string, 0)
	inputValuesMap := o.Inputs.ToMap()
	for inputName, cKey := range tmplCtx.InputsUsage().OAuthConfigsMap() {
		if len(inputValuesMap[inputName].Value.(map[string]interface{})) == 0 {
			warnings = append(warnings, fmt.Sprintf("- https://%s/admin/projects/%d/components/%s/%s", storageApiHost, projectID, cKey.ComponentId, cKey.Id))
		}
	}
	if len(warnings) > 0 {
		warnings = append([]string{"The template generated configurations that need additional oAuth authorization. Please follow the links and complete the setup:"}, warnings...)
	}

	logger.Info(fmt.Sprintf(`Template instance "%s" has been upgraded to "%s".`, o.Instance.InstanceId, tmpl.FullName()))
	return warnings, nil
}

type upgradedObject struct {
	model.ObjectState
	opMark string
}

type upgradedObjects []upgradedObject

func (v upgradedObjects) Log(logger log.Logger, tmpl *template.Template) {
	sort.SliceStable(v, func(i, j int) bool {
		return v[i].Path() < v[j].Path()
	})

	writer := logger.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Objects from "%s" template:`, tmpl.FullName()))
	for _, o := range v {
		writer.WriteStringIndent(1, fmt.Sprintf("%s %s %s", o.opMark, o.Kind().Abbr, o.Path()))
	}
}
