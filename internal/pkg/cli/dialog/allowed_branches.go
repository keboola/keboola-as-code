package dialog

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"
)

type branchesDialog struct {
	*Dialogs
	deps        branchesDialogDeps
	allBranches []*model.Branch
}

type branchesDialogDeps interface {
	Options() *options.Options
	StorageApiClient() client.Sender
}

func (p *Dialogs) AskAllowedBranches(ctx context.Context, deps branchesDialogDeps) (model.AllowedBranches, error) {
	return (&branchesDialog{Dialogs: p, deps: deps}).ask(ctx)
}

func (d *branchesDialog) ask(ctx context.Context) (model.AllowedBranches, error) {
	// Get Storage API
	storageApiClient := d.deps.StorageApiClient()

	// List all branches
	if v, err := storageapi.ListBranchesRequest().Send(ctx, storageApiClient); err == nil {
		for _, apiBranch := range *v {
			d.allBranches = append(d.allBranches, model.NewBranch(apiBranch))
		}
	} else {
		return nil, err
	}

	// Defined by flag
	if d.deps.Options().IsSet(`branches`) {
		value := d.deps.Options().GetString(`branches`)
		if value == "*" {
			return model.AllowedBranches{model.AllBranchesDef}, nil
		} else if value == "main" {
			return model.AllowedBranches{model.MainBranchDef}, nil
		}
		if allowedBranches := d.parseBranchesList(value, `,`); len(allowedBranches) > 0 {
			return allowedBranches, nil
		}
		return nil, fmt.Errorf(`please specify at least one branch`)
	}

	// Ask user
	switch d.askMode() {
	case ModeMainBranch:
		return model.AllowedBranches{model.MainBranchDef}, nil
	case ModeAllBranches:
		return model.AllowedBranches{model.AllBranchesDef}, nil
	case ModeSelectSpecific:
		if selectedBranches, err := d.SelectBranches(d.deps.Options(), d.allBranches, `Allowed project's branches:`); err == nil {
			return branchesToAllowedBranches(selectedBranches), nil
		} else {
			return nil, err
		}
	case ModeTypeList:
		if results := d.askBranchesList(); len(results) > 0 {
			return results, nil
		}
	}

	return nil, fmt.Errorf(`please specify at least one branch`)
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
		d.Printf("%s (%d)\n", branch.Name, branch.Id)
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
		Validator: func(val interface{}) error {
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
				return fmt.Errorf(`no existing project's branch matches your definitions`)
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
		out = append(out, model.AllowedBranch(b.Id.String()))
	}
	return out
}
