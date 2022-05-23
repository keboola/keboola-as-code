package upgrade

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	EncryptionApi() (*encryptionapi.Api, error)
}

func Run(projectState *project.State, tmpl *template.Template, o Options, d dependencies) error {
	logger := d.Logger()

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Create tickets provider, to generate new IDS, if needed
	tickets := storageApi.NewTicketProvider()

	// Load template
	ctx := upgrade.NewContext(d.Ctx(), tmpl.Reference(), tmpl.ObjectsRoot(), o.Instance.InstanceId, o.Branch, o.Inputs, tickets, projectState.State())
	templateState, err := tmpl.LoadState(ctx, use.LoadTemplateOptions())
	if err != nil {
		return err
	}

	// Prepare operations
	objects := make(upgradedObjects, 0)
	errors := utils.NewMultiError()
	renameOp := projectState.LocalManager().NewPathsGenerator(true)
	saveOp := projectState.LocalManager().NewUnitOfWork(projectState.Ctx())

	// Store template information in branch metadata
	branchState := projectState.GetOrNil(o.Branch).(*model.BranchState)

	// Get main config
	mainConfig, err := templateState.MainConfig()
	if err != nil {
		return err
	}

	// Update instance metadata
	if err := branchState.Local.Metadata.UpsertTemplateInstance(time.Now(), o.Instance.InstanceId, o.Instance.InstanceName, tmpl.TemplateId(), tmpl.Repository().Name, tmpl.Version(), storageApi.Token().Id, mainConfig); err != nil {
		errors.Append(err)
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
				errors.Append(err)
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
	if errors.Len() > 0 {
		return errors
	}
	if err := renameOp.Invoke(); err != nil {
		return err
	}
	if err := saveOp.Invoke(); err != nil {
		return err
	}

	// Encrypt values
	if err := encrypt.Run(projectState, encrypt.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(projectState.ProjectManifest(), projectState.Fs(), d); err != nil {
		return err
	}

	// Log new objects
	objects.Log(logger, tmpl)

	// Normalize paths
	if _, err := rename.Run(projectState, rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	// Validate schemas and encryption
	if err := validate.Run(projectState, validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(`Warning, ` + err.Error())
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	logger.Info(fmt.Sprintf(`Template instance "%s" has been upgraded to "%s".`, o.Instance.InstanceId, tmpl.FullName()))
	return nil
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
