package use

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/use"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	saveProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	InstanceName string
	TargetBranch model.BranchKey
	Inputs       template.InputsValues
	InstanceId   string
}

type newObjects []model.ObjectState

func (v newObjects) Log(logger log.Logger, tmpl *template.Template) {
	sort.SliceStable(v, func(i, j int) bool {
		return v[i].Path() < v[j].Path()
	})

	writer := logger.InfoWriter()
	writer.WriteString(fmt.Sprintf(`New objects from "%s" template:`, tmpl.FullName()))
	for _, o := range v {
		writer.WriteStringIndent(1, fmt.Sprintf("%s %s %s", diff.AddMark, o.Kind().Abbr, o.Path()))
	}
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

func LoadTemplateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(ctx context.Context, projectState *project.State, tmpl *template.Template, o Options, d dependencies) (instanceId string, warnings []string, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.local.template.use")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Get Storage API
	storageApiHost := d.StorageApiHost()
	tokenId := d.StorageApiTokenID()

	// Create tickets provider, to generate new IDS
	tickets := d.ObjectIDGeneratorFactory()(ctx)

	if o.InstanceId != "" {
		// Get instance ID from Options
		instanceId = o.InstanceId
	} else {
		// Generate ID for the template instance
		instanceId = idgenerator.TemplateInstanceId()
	}

	// Load template
	tmplCtx := use.NewContext(ctx, tmpl.Reference(), tmpl.ObjectsRoot(), instanceId, o.TargetBranch, o.Inputs, tmpl.Inputs().InputsMap(), tickets, d.Components())
	templateState, err := tmpl.LoadState(tmplCtx, loadState.LocalOperationOptions(), d)
	if err != nil {
		return "", nil, err
	}

	// Get manager
	manager := projectState.LocalManager()

	// Prepare operations
	objects := make(newObjects, 0)
	errs := errors.NewMultiError()
	renameOp := manager.NewPathsGenerator(true)
	saveOp := manager.NewUnitOfWork(ctx)

	// Store template information in branch metadata
	branchState := projectState.GetOrNil(o.TargetBranch).(*model.BranchState)

	// Get main config
	mainConfig, err := templateState.MainConfig()
	if err != nil {
		return "", nil, err
	}

	if err := branchState.Local.Metadata.UpsertTemplateInstance(time.Now(), instanceId, o.InstanceName, tmpl.TemplateId(), tmpl.Repository().Name, tmpl.Version(), tokenId, mainConfig); err != nil {
		errs.Append(err)
	}
	saveOp.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())

	// Rename and save all objects
	for _, objectState := range templateState.All() {
		// Skip branch - it is already processed
		if objectState.Kind().IsBranch() {
			continue
		}

		// Clear path
		objectState.Manifest().SetParentPath("")
		objectState.Manifest().SetRelativePath("")

		// Copy objects from template to project
		if err := projectState.Set(objectState); err != nil {
			errs.Append(err)
			continue
		}
		objects = append(objects, objectState)

		// Rename
		renameOp.Add(objectState)

		// Save to filesystem
		saveOp.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields())
	}

	if errs.Len() > 0 {
		return "", nil, errs
	}

	if err := renameOp.Invoke(); err != nil {
		return "", nil, err
	}
	if err := saveOp.Invoke(); err != nil {
		return "", nil, err
	}

	// Encrypt values
	if err := encrypt.Run(ctx, projectState, encrypt.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", nil, err
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(ctx, projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return "", nil, err
	}

	// Log new objects
	objects.Log(logger, tmpl)

	// Normalize paths
	if _, err := rename.Run(ctx, projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", nil, err
	}

	// Validate schemas and encryption
	if err := validate.Run(ctx, projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(`Warning, ` + err.Error())
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	// Return urls to oauth configurations
	warnings = make([]string, 0)
	for _, cKey := range tmplCtx.InputsUsage().OAuthConfigsMap() {
		warnings = append(warnings, fmt.Sprintf("- https://%s/admin/projects/%d/components/%s/%s", storageApiHost, d.ProjectID(), cKey.ComponentId, cKey.Id))
	}
	if len(warnings) > 0 {
		warnings = append([]string{"The template generated configurations that need oAuth authorization. Please follow the links and complete the setup:"}, warnings...)
	}

	logger.Info(fmt.Sprintf(`Template "%s" has been applied, instance ID: %s`, tmpl.FullName(), instanceId))
	return instanceId, warnings, nil
}
