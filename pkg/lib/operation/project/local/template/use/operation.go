package use

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
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
	InstanceName          string
	TargetBranch          model.BranchKey
	Inputs                template.InputsValues
	InstanceID            string
	SkipEncrypt           bool
	SkipSecretsValidation bool
}

type dependencies interface {
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	StorageAPIHost() string
	StorageAPITokenID() string
	Logger() log.Logger
	ObjectIDGeneratorFactory() func(ctx context.Context) *keboola.TicketProvider
	ProjectID() keboola.ProjectID
	ProjectBackends() []string
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func LoadTemplateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(ctx context.Context, projectState *project.State, tmpl *template.Template, o Options, d dependencies) (result *Result, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.template.use")
	defer span.End(&err)

	// Create tickets provider, to generate new IDS
	tickets := d.ObjectIDGeneratorFactory()(ctx)

	if o.InstanceID == "" {
		// Generate ID for the template instance
		o.InstanceID = idgenerator.TemplateInstanceID()
	}

	// Prepare template
	tmplCtx := use.NewContext(ctx, tmpl.Reference(), tmpl.ObjectsRoot(), o.InstanceID, o.TargetBranch, o.Inputs, tmpl.Inputs().InputsMap(), tickets, d.Components(), projectState.State(), d.ProjectBackends())
	plan, err := PrepareTemplate(ctx, d, ExtendedOptions{
		TargetBranch:          o.TargetBranch,
		Inputs:                o.Inputs,
		InstanceID:            o.InstanceID,
		InstanceName:          o.InstanceName,
		ProjectState:          projectState,
		Template:              tmpl,
		TemplateCtx:           tmplCtx,
		Upgrade:               false,
		SkipEncrypt:           o.SkipEncrypt,
		SkipSecretsValidation: o.SkipSecretsValidation,
	})
	if err != nil {
		return nil, err
	}

	return plan.Invoke(ctx)
}

type ExtendedOptions struct {
	TargetBranch          model.BranchKey
	Inputs                template.InputsValues
	InstanceID            string
	InstanceName          string
	ConfigName            string
	ProjectState          *project.State
	Template              *template.Template
	TemplateCtx           template.Context
	Upgrade               bool
	SkipEncrypt           bool
	SkipSecretsValidation bool
}

type TemplatePlan struct {
	options       ExtendedOptions
	deps          dependencies
	templateState *template.State
	renameOp      *local.PathsGenerator
	saveOp        *local.UnitOfWork
	modified      ModifiedObjects
	result        *Result
}

type Result struct {
	InstanceID string
	ConfigID   string
	Warnings   []string
}

func PrepareTemplate(ctx context.Context, d dependencies, o ExtendedOptions) (plan *TemplatePlan, err error) {
	errs := errors.NewMultiError()
	existingObjects := make(map[model.Key]bool)
	manager := o.ProjectState.LocalManager()
	plan = &TemplatePlan{
		options:  o,
		deps:     d,
		renameOp: manager.NewPathsGenerator(true),
		saveOp:   manager.NewUnitOfWork(ctx),
	}

	// Load template state
	plan.templateState, err = o.Template.LoadState(o.TemplateCtx, LoadTemplateOptions(), d)
	if err != nil {
		var invalidState loadState.InvalidLocalStateError
		if errors.As(err, &invalidState) {
			// Strip confusing  local state is invalid:" error envelope.
			return nil, errors.PrefixError(invalidState.Unwrap(), "template definition is not valid")
		}
		return nil, err
	}

	// Get main config
	mainConfig, err := plan.templateState.MainConfig()
	if err != nil {
		return nil, err
	}

	// Load existing shared codes
	sharedCodes := make(map[keboola.ComponentID]*model.ConfigState)
	for _, config := range o.ProjectState.ConfigsFrom(o.TargetBranch) {
		if config.ComponentID == keboola.SharedCodeComponentID {
			sharedCodes[config.Local.SharedCode.Target] = config
			existingObjects[config.Key()] = true
		}
	}

	// Update instance metadata
	branchState := o.ProjectState.GetOrNil(o.TargetBranch).(*model.BranchState)
	if err := branchState.Local.Metadata.UpsertTemplateInstance(time.Now(), o.InstanceID, o.InstanceName, o.Template.TemplateID(), o.Template.Repository().Name, o.Template.Version(), d.StorageAPITokenID(), mainConfig); err != nil {
		errs.Append(err)
	}
	plan.saveOp.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())

	// Save all objects
	for _, tmplObjectState := range plan.templateState.All() {
		// Skip branch - it is already processed
		if tmplObjectState.Kind().IsBranch() {
			continue
		}

		if mergeSharedCodes(tmplObjectState, plan, sharedCodes) {
			continue
		}

		// Create or update the object
		var opMark string
		var objectState model.ObjectState
		if v, found := o.ProjectState.Get(tmplObjectState.Key()); found {
			opMark = diff.ChangeMark
			objectState = v
			objectState.SetLocalState(tmplObjectState.LocalState())

			// Clear path
			objectState.Manifest().SetParentPath("")
			objectState.Manifest().SetRelativePath("")
			plan.renameOp.Add(objectState)
		} else {
			opMark = diff.AddMark
			objectState = tmplObjectState

			// Clear path
			objectState.Manifest().SetParentPath("")
			objectState.Manifest().SetRelativePath("")

			// Copy state from template to project
			if err := o.ProjectState.Set(objectState); err != nil {
				errs.Append(err)
				continue
			}

			// Generate path
			plan.renameOp.Add(objectState)
		}
		existingObjects[objectState.Key()] = true
		plan.modified = append(plan.modified, ModifiedObject{ObjectState: objectState, OpMark: opMark})

		if objectState.Kind().IsConfig() {
			// Change config name that is used in the template instead of meta.json one
			if plan.options.ConfigName != "" {
				config := objectState.(*model.ConfigState)
				config.Local.Name = plan.options.ConfigName
			}

			// Save config ID
			if plan.result == nil {
				plan.result = &Result{ConfigID: objectState.ObjectID()}
			}
		}

		// Save to filesystem
		plan.saveOp.SaveObject(objectState, objectState.LocalState(), model.NewChangedFields())
	}

	// Delete
	if o.Upgrade {
		var toDelete []model.Key
		configs := search.ConfigsForTemplateInstance(o.ProjectState.LocalObjects().ConfigsWithRowsFrom(o.TargetBranch), o.InstanceID)
		for _, config := range configs {
			if !existingObjects[config.Key()] {
				toDelete = append(toDelete, config.Key())
			}
			for _, row := range config.Rows {
				if !existingObjects[row.Key()] {
					toDelete = append(toDelete, row.Key())
				}
			}
		}
		for _, key := range toDelete {
			objectState := o.ProjectState.MustGet(key)
			plan.saveOp.DeleteObject(objectState, objectState.Manifest())
			plan.modified = append(plan.modified, ModifiedObject{ObjectState: objectState, OpMark: diff.DeleteMark})
		}
	}

	return plan, errs.ErrorOrNil()
}

func (p *TemplatePlan) Invoke(ctx context.Context) (*Result, error) {
	logger := p.deps.Logger()

	if err := p.renameOp.Invoke(); err != nil {
		return nil, err
	}

	if err := p.saveOp.Invoke(); err != nil {
		return nil, err
	}

	// Encrypt values
	if !p.options.SkipEncrypt {
		if err := encrypt.Run(ctx, p.options.ProjectState, encrypt.Options{DryRun: false, LogEmpty: false}, p.deps); err != nil {
			return nil, err
		}
	}

	// Save manifest
	if _, err := saveProjectManifest.Run(ctx, p.options.ProjectState.ProjectManifest(), p.options.ProjectState.Fs(), p.deps); err != nil {
		return nil, err
	}

	// Log new objects
	p.modified.Log(p.deps.Stdout(), p.options.Template)

	// Normalize paths
	if _, err := rename.Run(ctx, p.options.ProjectState, rename.Options{DryRun: false, LogEmpty: false}, p.deps); err != nil {
		return nil, err
	}

	// Validate schemas and encryption
	if err := validate.Run(ctx, p.options.ProjectState, validate.Options{ValidateSecrets: !p.options.SkipSecretsValidation, ValidateJSONSchema: true}, p.deps); err != nil {
		logger.Warn(ctx, errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
		logger.Warn(ctx, "")
		logger.Warn(ctx, `Please correct the problems listed above.`)
		logger.Warn(ctx, `Push operation is only possible when project is valid.`)
	}

	result := p.result
	if result == nil {
		result = &Result{InstanceID: p.options.InstanceID}
	} else {
		result.InstanceID = p.options.InstanceID
	}

	// Return urls to oauth configurations
	if tmplCtx, ok := p.options.TemplateCtx.(interface{ InputsUsage() *metadata.InputsUsage }); ok {
		var oauthWarnings []string
		inputValuesMap := p.options.Inputs.ToMap()
		for inputName, cKey := range tmplCtx.InputsUsage().OAuthConfigsMap() {
			if len(inputValuesMap[inputName].Value.(map[string]any)) == 0 {
				oauthWarnings = append(oauthWarnings, fmt.Sprintf("- %s/admin/projects/%d/components/%s/%s", p.deps.StorageAPIHost(), p.deps.ProjectID(), cKey.ComponentID, cKey.ID))
			}
		}
		if len(oauthWarnings) > 0 {
			result.Warnings = append([]string{"The template generated configurations that need oAuth authorization. Please follow the links and complete the setup:"}, oauthWarnings...)
		}
	}

	// Log success
	if p.options.Upgrade {
		logger.Infof(ctx, `Template instance "%s" has been upgraded to "%s".`, p.options.InstanceID, p.options.Template.FullName())
	} else {
		logger.Infof(ctx, `Template "%s" has been applied, instance ID: %s`, p.options.Template.FullName(), p.options.InstanceID)
	}

	return result, nil
}

func mergeSharedCodes(object model.ObjectState, plan *TemplatePlan, sharedCodes map[keboola.ComponentID]*model.ConfigState) (skip bool) {
	switch v := object.(type) {
	case *model.ConfigState:
		if v.Local.SharedCode != nil {
			if sharedCodeConfig, exists := sharedCodes[v.Local.SharedCode.Target]; exists {
				// Shared code config already exists, merge metadata
				for k, v := range v.Local.Metadata {
					if _, found := sharedCodeConfig.Local.Metadata[k]; !found {
						sharedCodeConfig.Local.Metadata[k] = v
					}
				}
				return true
			}
		}
		if v.Local.Transformation != nil && v.Local.Transformation.LinkToSharedCode != nil {
			if sharedCodeConfig, exists := sharedCodes[v.Local.ComponentID]; exists {
				// Update references to shared codes in the transformation
				v.Local.Transformation.LinkToSharedCode.Config = sharedCodeConfig.ConfigKey
				for i := range v.Local.Transformation.LinkToSharedCode.Rows {
					v.Local.Transformation.LinkToSharedCode.Rows[i].ConfigID = sharedCodeConfig.ID
				}
				for _, block := range v.Local.Transformation.Blocks {
					for _, code := range block.Codes {
						for i, script := range code.Scripts {
							if v, ok := script.(model.LinkScript); ok {
								v.Target.ConfigID = sharedCodeConfig.ID
								code.Scripts[i] = v
							}
						}
					}
				}
			}
		}
		if v.ComponentID == keboola.VariablesComponentID {
			for i := range v.Local.Relations {
				rel := v.Local.Relations[i]
				if rel, ok := rel.(*model.SharedCodeVariablesForRelation); ok {
					// Update references in the variables config
					varUsedInKey := model.ConfigKey{BranchID: v.BranchID, ComponentID: keboola.SharedCodeComponentID, ID: rel.ConfigID}
					varUsedIn := plan.templateState.MustGet(varUsedInKey).(*model.ConfigState)
					if sharedCodeConfig, exists := sharedCodes[varUsedIn.Local.SharedCode.Target]; exists {
						rel.ConfigID = sharedCodeConfig.ID
					}
				}
			}
		}
	case *model.ConfigRowState:
		if v.ComponentID == keboola.SharedCodeComponentID {
			if sharedCodeConfig, exists := sharedCodes[v.Local.SharedCode.Target]; exists {
				// Attach the config row to the existing shared code
				v.ConfigID = sharedCodeConfig.ID
				v.Local.ConfigID = sharedCodeConfig.ID
			}
		}
	}
	return false
}

// ModifiedObject for logs.
type ModifiedObject struct {
	model.ObjectState
	OpMark string
}

type ModifiedObjects []ModifiedObject

func (v ModifiedObjects) Log(w io.Writer, tmpl *template.Template) {
	sort.SliceStable(v, func(i, j int) bool {
		return v[i].Path() < v[j].Path()
	})

	fmt.Fprintf(w, `Objects from "%s" template:`, tmpl.FullName())
	fmt.Fprintln(w)
	for _, o := range v {
		fmt.Fprintf(w, "  %s %s %s", o.OpMark, o.Kind().Abbr, o.Path())
		fmt.Fprintln(w)
	}
}
