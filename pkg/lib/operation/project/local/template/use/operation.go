package use

import (
	"context"
	"fmt"
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	saveProjectManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
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
	StorageApi() (*storageapi.Api, error)
	EncryptionApi() (*encryptionapi.Api, error)
}

func LoadTemplateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(projectState *project.State, tmpl *template.Template, o Options, d dependencies) (string, error) {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return "", err
	}

	// Create tickets provider, to generate new IDS
	tickets := storageApi.NewTicketProvider()

	// Generate ID for the template instance
	instanceId := idgenerator.TemplateInstanceId()

	// Load template
	ctx := template.NewUseContext(d.Ctx(), tmpl.Reference(), tmpl.ObjectsRoot(), instanceId, o.TargetBranch, o.Inputs, tickets)
	templateState, err := tmpl.LoadState(ctx, LoadTemplateOptions())
	if err != nil {
		return "", err
	}

	// Prepare operations
	objects := make(newObjects, 0)
	errors := utils.NewMultiError()
	renameOp := projectState.LocalManager().NewPathsGenerator(true)
	saveOp := projectState.LocalManager().NewUnitOfWork(projectState.Ctx())

	// Store template information in branch metadata
	branchState := projectState.GetOrNil(o.TargetBranch).(*model.BranchState)
	version := tmpl.Version()
	if err := branchState.Local.Metadata.AddTemplateUsage(instanceId, tmpl.TemplateId(), version.String()); err != nil {
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
		return "", errors
	}
	if err := renameOp.Invoke(); err != nil {
		return "", err
	}
	if err := saveOp.Invoke(); err != nil {
		return "", err
	}

	// Encrypt values
	if err := encrypt.Run(projectState, encrypt.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", err
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return "", err
	}

	// Log new objects
	objects.Log(logger, tmpl)

	// Normalize paths
	if _, err := rename.Run(projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return "", err
	}

	// Validate schemas and encryption
	if err := validate.Run(projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(`Warning, ` + err.Error())
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	logger.Info(fmt.Sprintf(`Template "%s" has been applied, instance ID: %s`, tmpl.FullName(), instanceId))
	return instanceId, nil
}
