package use

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/rename"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	TargetBranch model.BranchId
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	TemplateState(loadOptions loadState.OptionsWithFilter, replacements replacekeys.Keys) (*template.State, error)
	ProjectDir() (filesystem.Fs, error)
	ProjectManifest() (*project.Manifest, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
}

func LoadTemplateOptions() loadState.OptionsWithFilter {
	return loadState.OptionsWithFilter{
		Options: loadState.Options{
			LoadLocalState:    true,
			LoadRemoteState:   false,
			IgnoreNotFoundErr: false,
		},
	}
}

func LoadProjectOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(o Options, d dependencies) error {
	logger := d.Logger()

	// Load template
	templateState, err := d.TemplateState(LoadTemplateOptions(), nil)
	if err != nil {
		return err
	}

	// Load project
	projectState, err := d.ProjectState(LoadProjectOptions())
	if err != nil {
		return err
	}

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return err
	}

	// Generate keys
	ticketProvider := storageApi.NewTicketProvider()
	newConfigIds := make(map[model.ConfigId]model.ConfigId)
	var keys replacekeys.Keys

	keys = append(keys, replacekeys.Key{Old: model.BranchKey{Id: 0}, New: model.BranchKey{Id: o.TargetBranch}})

	for _, object := range templateState.All() {
		switch v := object.(type) {
		case *model.ConfigState:
			ticketProvider.Request(func(ticket *model.Ticket) {
				oldKey := v.ConfigKey
				newKey := oldKey
				newKey.BranchId = o.TargetBranch
				newKey.Id = model.ConfigId(ticket.Id)
				newConfigIds[oldKey.Id] = newKey.Id
				keys = append(keys, replacekeys.Key{Old: oldKey, New: newKey})
			})
		case *model.ConfigRowState:
			ticketProvider.Request(func(ticket *model.Ticket) {
				oldKey := v.ConfigRowKey
				newKey := oldKey
				newKey.BranchId = o.TargetBranch
				newKey.ConfigId = newConfigIds[oldKey.ConfigId]
				newKey.Id = model.RowId(ticket.Id)
				keys = append(keys, replacekeys.Key{Old: oldKey, New: newKey})
			})
		default:
			panic(fmt.Errorf(`unexpected object type "%T"`, object))
		}
	}

	// Get tickets
	if err := ticketProvider.Resolve(); err != nil {
		return err
	}

	// Convert keys to values
	values, err := keys.Values()
	if err != nil {
		return err
	}

	// UOW
	renameOp := projectState.LocalManager().NewPathsGenerator(true)
	saveOp := projectState.LocalManager().NewUnitOfWork(projectState.Ctx())

	// Replace keys
	errors := utils.NewMultiError()
	for _, original := range templateState.All() {
		modified := replacekeys.ReplaceValues(values, original).(model.ObjectState)
		if err := projectState.Set(modified); err != nil {
			errors.Append(err)
		}
		renameOp.Add(modified)
		saveOp.SaveObject(modified, modified.LocalState(), model.NewChangedFields())
	}
	if errors.Len() > 0 {
		return errors
	}
	if err := renameOp.Invoke(); err != nil {
		return err
	}
	if err := saveOp.Invoke(); err != nil {
		return err
	}

	// Save manifest
	if _, err := saveManifest.Run(d); err != nil {
		return err
	}

	// Normalize paths
	if _, err := rename.Run(rename.Options{DryRun: false, LogEmpty: false}, d); err != nil {
		return err
	}

	// Validate schemas and encryption
	if err := validate.Run(validate.Options{ValidateSecrets: true, ValidateJsonSchema: true}, d); err != nil {
		logger.Warn(`Warning, ` + err.Error())
		logger.Warn()
		logger.Warnf(`Please correct the problems listed above.`)
		logger.Warnf(`Push operation is only possible when project is valid.`)
	}

	return nil
}
