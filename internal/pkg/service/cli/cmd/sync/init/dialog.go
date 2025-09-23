package init

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci/workflow"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"

	WorkflowModeSkip     = "skip"
	WorkflowModeSet      = "set"
	WorkflowModeInteract = "interact"
)

type initDeps interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func AskInitOptions(ctx context.Context, d *dialog.Dialogs, dep initDeps, f Flags) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming:         naming.TemplateWithoutIds(),
			AllowTargetENV: f.AllowTargetENV.Value,
		},
	}

	// Allowed branches
	if allowedBranches, err := AskAllowedBranches(ctx, dep, d, f); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	// Ask for workflows options
	switch determineWorkflowMode(f, d) {
	case WorkflowModeSkip:
		d.Printf("Skipping GitHub workflow setup as requested by --skip-workflows flag.\n")
	case WorkflowModeSet:
		if f.CIValidate.IsSet() || f.CIPush.IsSet() || f.CIPull.IsSet() {
			return out, errors.New("`ci-*` flags may not be set if `ci` is set to `false`")
		}

		out.Workflows = workflowsGen.Options{
			Validate:   f.CI.Value,
			Push:       f.CI.Value,
			Pull:       f.CI.Value,
			MainBranch: f.CIMainBranch.Value,
		}
	case WorkflowModeInteract:
		out.Workflows = workflow.AskWorkflowsOptions(workflow.Flags{
			CI:           f.CI,
			CIPush:       f.CIPush,
			CIPull:       f.CIPull,
			CIMainBranch: f.CIMainBranch,
			CIValidate:   f.CIValidate,
		}, d)
	}

	return out, nil
}

func determineWorkflowMode(f Flags, d *dialog.Dialogs) string {
	if f.SkipWorkflows.Value {
		return WorkflowModeSkip
	}
	if f.CI.IsSet() {
		return WorkflowModeSet
	}
	if d.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		return WorkflowModeInteract
	}
	return WorkflowModeSkip
}

type branchesDialog struct {
	*dialog.Dialogs
	Flags
	deps        branchesDialogDeps
	allBranches []*model.Branch
}

type branchesDialogDeps interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func AskAllowedBranches(ctx context.Context, deps branchesDialogDeps, d *dialog.Dialogs, f Flags) (model.AllowedBranches, error) {
	result, err := (&branchesDialog{Dialogs: d, deps: deps, Flags: f}).ask(ctx)
	if err != nil {
		return model.AllowedBranches{}, err
	}

	// Check that only one branch is specified if the flag is used
	if f.AllowTargetENV.Value && !result.IsOneSpecificBranch() {
		return nil, errors.Errorf(`flag --allow-target-env can only be used with one specific branch, found %s`, result.String())
	}

	return result, nil
}

func (d *branchesDialog) ask(ctx context.Context) (model.AllowedBranches, error) {
	// Get Storage API
	api := d.deps.KeboolaProjectAPI()

	// List all branches
	if v, err := api.ListBranchesRequest().Send(ctx); err == nil {
		for _, apiBranch := range *v {
			d.allBranches = append(d.allBranches, model.NewBranch(apiBranch))
		}
	} else {
		return nil, err
	}

	// Defined by flag
	if d.Branches.IsSet() {
		value := d.Branches.Value
		switch value {
		case "*":
			return model.AllowedBranches{model.AllBranchesDef}, nil
		case "main":
			return model.AllowedBranches{model.MainBranchDef}, nil
		}
		if allowedBranches := d.parseBranchesList(value, `,`); len(allowedBranches) > 0 {
			return allowedBranches, nil
		}
		return nil, errors.New(`please specify at least one branch`)
	}

	// Ask user
	switch d.askMode() {
	case ModeMainBranch:
		return model.AllowedBranches{model.MainBranchDef}, nil
	case ModeAllBranches:
		return model.AllowedBranches{model.AllBranchesDef}, nil
	case ModeSelectSpecific:
		if selectedBranches, err := SelectBranches(d.allBranches, `Allowed project's branches:`, d.Dialogs, d.Flags); err == nil {
			return branchesToAllowedBranches(selectedBranches), nil
		} else {
			return nil, err
		}
	case ModeTypeList:
		if results := d.askBranchesList(); len(results) > 0 {
			return results, nil
		}
	}

	return nil, errors.New(`please specify at least one branch`)
}

func (d *branchesDialog) askMode() string {
	var description string
	var options []string
	if d.AllowTargetENV.Value {
		description = "Please select project's branch you want to use with this CLI.\nThe other branches will still exist, but they will be invisible in the CLI."
		options = []string{
			ModeMainBranch,
			ModeSelectSpecific,
		}
	} else {
		description = "Please select project's branches you want to use with this CLI.\nThe other branches will still exist, but they will be invisible in the CLI."
		options = []string{
			ModeMainBranch,
			ModeAllBranches,
			ModeSelectSpecific,
			ModeTypeList,
		}
	}

	mode, _ := d.Select(&prompt.Select{
		Label:       "Allowed project's branches:",
		Description: description,
		Options:     options,
		Default:     ModeMainBranch,
	})

	return mode
}

func (d *branchesDialog) askBranchesList() model.AllowedBranches {
	// Print first 10 branches for inspiration
	end := math.Min(10, float64(len(d.allBranches)))
	d.Printf("\nExisting project's branches, for inspiration:\n")
	for _, branch := range d.allBranches[:int(end)] {
		d.Printf("%s (%d)\n", branch.Name, branch.ID)
	}
	if len(d.allBranches) > 10 {
		d.Printf(`...`)
	}

	// Prompt
	lines, ok := d.Multiline(&prompt.Question{
		Label: "Allowed project's branches:",
		Description: "\nPlease enter one branch definition per line.\n" +
			"Each definition can be:\n" +
			"- branch ID\n" +
			"- branch name, with optional wildcards, eg. \"Foo Bar\", \"Dev:*\"\n" +
			"- branch directory (normalized) name, with optional wildcards, eg. \"foo-bar\", \"dev-*\"\n",
		Validator: func(val any) error {
			// At least one existing branch must match user definition
			matched := 0
			for _, branch := range d.allBranches {
				for _, definition := range d.parseBranchesList(val.(string), "\n") {
					if definition.IsBranchAllowed(branch) {
						matched++
					}
				}
			}
			if matched == 0 {
				return errors.New(`no existing project's branch matches your definitions`)
			}
			return nil
		},
	})

	if !ok {
		return nil
	}

	// Normalize
	return d.parseBranchesList(lines, "\n")
}

func (d *branchesDialog) parseBranchesList(str, sep string) model.AllowedBranches {
	branches := model.AllowedBranches{}
	for item := range strings.SplitSeq(str, sep) {
		item = strings.TrimSpace(item)
		if len(item) == 0 {
			continue
		}
		branches = append(branches, model.AllowedBranch(item))
	}
	return d.unique(branches)
}

// unique returns only unique items.
func (d *branchesDialog) unique(items model.AllowedBranches) model.AllowedBranches {
	m := orderedmap.New()
	for _, item := range items {
		m.Set(string(item), true)
	}

	unique := model.AllowedBranches{}
	for _, item := range m.Keys() {
		unique = append(unique, model.AllowedBranch(item))
	}
	return unique
}

func branchesToAllowedBranches(branches []*model.Branch) (out model.AllowedBranches) {
	for _, b := range branches {
		out = append(out, model.AllowedBranch(b.ID.String()))
	}
	return out
}

func SelectBranches(all []*model.Branch, label string, d *dialog.Dialogs, f Flags) (results []*model.Branch, err error) {
	if f.Branches.IsSet() {
		errs := errors.NewMultiError()
		for item := range strings.SplitSeq(f.Branches.Value, `,`) {
			item = strings.TrimSpace(item)
			if len(item) == 0 {
				continue
			}

			if b, err := search.Branch(all, item); err == nil {
				results = append(results, b)
			} else {
				errs.Append(err)
				continue
			}
		}
		if len(results) > 0 {
			return results, errs.ErrorOrNil()
		}
		return nil, errors.New(`please specify at least one branch`)
	}

	selectOpts := orderedmap.New()
	for _, branch := range all {
		msg := fmt.Sprintf(`%s (%d)`, branch.Name, branch.ID)
		selectOpts.Set(msg, branch.ID)
	}

	if f.AllowTargetENV.Value {
		index, _ := d.SelectIndex(&prompt.SelectIndex{
			Label:       label,
			Description: "Please select one branch.",
			Options:     selectOpts.Keys(),
		})
		results = append(results, all[index])
	} else {
		indexes, _ := d.MultiSelectIndex(&prompt.MultiSelectIndex{
			Label:       label,
			Description: "Please select one or more branches.",
			Options:     selectOpts.Keys(),
			Validator:   prompt.AtLeastOneRequired,
		})
		for _, index := range indexes {
			results = append(results, all[index])
		}
	}

	if len(results) > 0 {
		return results, nil
	}

	return nil, errors.New(`please specify at least one branch`)
}
