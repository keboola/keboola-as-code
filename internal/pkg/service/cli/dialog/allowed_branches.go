package dialog

import (
	"context"
	"math"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"
)

type BranchesDialogDeps interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

type branchesDialog struct {
	*Dialogs
	deps           BranchesDialogDeps
	allBranches    []*model.Branch
	branches       configmap.Value[string]
	allowTargetENV configmap.Value[bool]
}

// AskAllowedBranches can be used to ask for "allowedBranches" for a manifest.
func (p *Dialogs) AskAllowedBranches(ctx context.Context, deps BranchesDialogDeps, branches configmap.Value[string], allowTargetENV configmap.Value[bool]) (model.AllowedBranches, error) {
	result, err := (&branchesDialog{
		Dialogs:        p,
		deps:           deps,
		allowTargetENV: allowTargetENV,
		branches:       branches,
	}).ask(ctx)
	if err != nil {
		return model.AllowedBranches{}, err
	}

	// Check that only one branch is specified if the flag is used
	if allowTargetENV.Value && !result.IsOneSpecificBranch() {
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
	if d.branches.IsSet() {
		value := d.branches.Value
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
		if selectedBranches, err := d.SelectBranches(d.allBranches, `Allowed project's branches:`, d.branches, d.allowTargetENV); err == nil {
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
	if d.allowTargetENV.Value {
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
