package init

import (
	"context"
	"math"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type initDeps interface {
	KeboolaProjectAPI() *keboola.API
}

func AskInitOptions(ctx context.Context, a *dialog.Dialogs, d initDeps, flags Flags) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming: naming.TemplateWithoutIds(),
		},
	}

	// Allowed branches
	if allowedBranches, err := AskAllowedBranches(ctx, a, d, flags); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	// Ask for workflows options
	if flags.CI.IsSet() {
		if flags.CIValidate.IsSet() || flags.CIPush.IsSet() || flags.CIPull.IsSet() {
			return out, errors.New("`ci-*` flags may not be set if `ci` is set to `false`")
		}

		out.Workflows = workflowsGen.Options{
			Validate:   flags.CI.Value,
			Push:       flags.CI.Value,
			Pull:       flags.CI.Value,
			MainBranch: flags.CIMainBranch.Value,
		}
	} else if a.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		out.Workflows = a.AskWorkflowsOptions()
	}

	return out, nil
}

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"
)

type branchesDialog struct {
	*dialog.Dialogs
	deps        branchesDialogDeps
	allBranches []*model.Branch
}

type branchesDialogDeps interface {
	KeboolaProjectAPI() *keboola.API
}

func AskAllowedBranches(ctx context.Context, d *dialog.Dialogs, deps branchesDialogDeps, f Flags) (model.AllowedBranches, error) {
	return (&branchesDialog{Dialogs: d, deps: deps}).ask(ctx, f)
}

func (d *branchesDialog) ask(ctx context.Context, f Flags) (model.AllowedBranches, error) {
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
	if f.Branches.IsSet() {
		value := f.Branches.Value
		if value == "*" {
			return model.AllowedBranches{model.AllBranchesDef}, nil
		} else if value == "main" {
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
		if selectedBranches, err := d.SelectBranches(d.allBranches, `Allowed project's branches:`); err == nil {
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
	mode, _ := d.Select(&prompt.Select{
		Label: "Allowed project's branches:",
		Description: "Please select which project's branches you want to use with this CLI.\n" +
			"The other branches will still exist, but they will be invisible in the CLI.",
		Options: []string{
			ModeMainBranch,
			ModeAllBranches,
			ModeSelectSpecific,
			ModeTypeList,
		},
		Default: ModeMainBranch,
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
	for _, item := range strings.Split(str, sep) {
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
