package use

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/use"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
	Ctx() context.Context
	Logger() log.Logger
	ProjectID() (int, error)
	StorageApiHost() (string, error)
	StorageAPITokenID() (string, error)
	StorageApiClient() (client.Sender, error)
	EncryptionApiClient() (client.Sender, error)
}

func LoadTemplateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(projectState *project.State, tmpl *template.Template, o Options, d dependencies) (string, []string, error) {
	logger := d.Logger()

	// Get Storage API
	storageApiClient, err := d.StorageApiClient()
	if err != nil {
		return "", nil, err
	}

	// Host
	storageApiHost, err := d.StorageApiHost()
	if err != nil {
		return "", nil, err
	}

	// Project ID
	projectID, err := d.ProjectID()
	if err != nil {
		return "", nil, err
	}

	// Token ID
	tokenId, err := d.StorageAPITokenID()
	if err != nil {
		return "", nil, err
	}

	// Create tickets provider, to generate new IDS
	tickets := storageapi.NewTicketProvider(d.Ctx(), storageApiClient)

	// Generate ID for the template instance
	instanceId := idgenerator.TemplateInstanceId()

	// Load template
	ctx := use.NewContext(d.Ctx(), tmpl.Reference(), tmpl.ObjectsRoot(), instanceId, o.TargetBranch, o.Inputs, tmpl.Inputs().InputsMap(), tickets)
	templateState, err := tmpl.LoadState(ctx, LoadTemplateOptions())
	if err != nil {
		return "", nil, err
	}

	// Prepare operations
	objects := make(newObjects, 0)
	errors := utils.NewMultiError()
	renameOp := projectState.LocalManager().NewPathsGenerator(true)
	saveOp := projectState.LocalManager().NewUnitOfWork(projectState.Ctx())

	// Store template information in branch metadata
	branchState := projectState.GetOrNil(o.TargetBranch).(*model.BranchState)

	// Get main config
	mainConfig, err := templateState.MainConfig()
	if err != nil {
		return "", nil, err
	}

	if err := branchState.Local.Metadata.UpsertTemplateInstance(time.Now(), instanceId, o.InstanceName, tmpl.TemplateId(), tmpl.Repository().Name, tmpl.Version(), tokenId, mainConfig); err != nil {
		errors.Append(err)
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
			errors.Append(err)
			continue
		}
		objects = append(objects, objectState)

		// Rename
		renameOp.Add(objectState)

		// Save to filesystem
		saveOp.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields())
	}

	if errors.Len() > 0 {
		return "", nil, errors
	}
	if err := renameOp.Invoke(); err != nil {
		return "", nil, err
	}
	if err := saveOp.Invoke(); err != nil {
		return "", nil, err
	}

	// Encrypt values
	if err := encrypt.Run(projectState, encrypt.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", nil, err
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return "", nil, err
	}

	// Log new objects
	objects.Log(logger, tmpl)

	// Normalize paths
	if _, err := rename.Run(projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", nil, err
	}

	// Validate schemas and encryption
	if err := validate.Run(projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(`Warning, ` + err.Error())
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	// Return urls to oauth configurations
	warnings := make([]string, 0)
	for _, cKey := range ctx.InputsUsage().OAuthConfigsMap() {
		warnings = append(warnings, fmt.Sprintf("- https://%s/admin/projects/%d/components/%s/%s", storageApiHost, projectID, cKey.ComponentId, cKey.Id))
	}
	if len(warnings) > 0 {
		warnings = append([]string{"The template generated configurations that need oAuth authorization. Please follow the links and complete the setup:"}, warnings...)
	}

	logger.Info(fmt.Sprintf(`Template "%s" has been applied, instance ID: %s`, tmpl.FullName(), instanceId))
	return instanceId, warnings, nil
}
