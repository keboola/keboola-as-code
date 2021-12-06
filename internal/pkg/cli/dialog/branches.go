package dialog

import (
	"fmt"
	"math"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"
)

type branchesDialog struct {
	prompt      prompt.Prompt
	allBranches []*model.Branch
	isFlagSet   bool
	flagValue   string
	defaultMode string
}

type branchesDialogDeps interface {
	Options() *options.Options
	StorageApi() (*remote.StorageApi, error)
}

func (p *Dialogs) AskAllowedBranches(d branchesDialogDeps) (model.AllowedBranches, error) {
	o := d.Options()
	isFlagSet := o.IsSet("allowed-branches")
	flagValue := o.GetString("allowed-branches")

	// Convert CLI values to internal
	if flagValue == "*" {
		flagValue = model.AllBranchesDef
	} else if flagValue == "main" {
		flagValue = model.MainBranchDef
	}

	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return nil, err
	}

	// List all branches
	allBranches, err := storageApi.ListBranches()
	if err != nil {
		return nil, err
	}

	return (&branchesDialog{
		prompt:      p.Prompt,
		allBranches: allBranches,
		isFlagSet:   isFlagSet,
		flagValue:   flagValue,
		defaultMode: ModeMainBranch,
	}).ask(), nil
}

func (d *branchesDialog) ask() model.AllowedBranches {
	if d.prompt.IsInteractive() && !d.isFlagSet {
		for {
			mode, results := d.doAsk()

			// Check that the definition meets at least one branch
			matched := 0
			for _, branch := range d.allBranches {
				for _, definition := range results {
					if definition.IsBranchAllowed(branch) {
						matched++
					}
				}
			}

			// If a branch is matched -> ok
			if matched > 0 {
				if mode == ModeTypeList {
					if matched == 1 {
						d.prompt.Printf("\nOne project's branch match defined \"allowed branches\". Ok.\n")
					} else {
						d.prompt.Printf("\n%d project's branches match defined \"allowed branches\". Ok.\n", matched)
					}
				}
				return results
			}

			d.prompt.Printf("\nNo existing project's branch matches your definitions. Please try again.\n")
		}
	}

	// Parse flag value
	return d.parseString(d.flagValue, ",")
}

func (d *branchesDialog) doAsk() (string, model.AllowedBranches) {
	// Ask user for mode
	mode, ok := d.prompt.Select(&prompt.Select{
		Label: "Allowed project's branches:",
		Description: "Please select which project's branches you want to use with this CLI.\n" +
			"The other branches will still exist, but they will be invisible in the CLI.",
		Options: []string{
			ModeMainBranch,
			ModeAllBranches,
			ModeSelectSpecific,
			ModeTypeList,
		},
		Default: d.defaultMode,
	})

	// If it is necessary to repeat the selection (an error occurs), the option will be used as default
	d.defaultMode = mode

	// Load definitions according to the specified mode
	if ok {
		switch mode {
		case ModeMainBranch:
			return mode, model.AllowedBranches{model.MainBranchDef}
		case ModeAllBranches:
			return mode, model.AllowedBranches{model.AllBranchesDef}
		case ModeSelectSpecific:
			return mode, d.selectBranches()
		case ModeTypeList:
			return mode, d.typeBranchesList()
		}
	}

	return mode, nil
}

func (d *branchesDialog) selectBranches() model.AllowedBranches {
	// Build options
	o := orderedmap.New()
	for _, branch := range d.allBranches {
		msg := fmt.Sprintf(`[%d] %s`, branch.Id, branch.Name)
		o.Set(msg, branch.Id)
	}

	// Prompt
	keys, ok := d.prompt.MultiSelect(&prompt.MultiSelect{
		Label:       "Allowed project's branches:",
		Description: "Please select one or more branches.",
		Options:     o.Keys(),
	})

	if !ok {
		return nil
	}

	ids := make([]string, 0)
	for _, key := range keys {
		v, found := o.Get(key)
		if !found {
			panic(fmt.Errorf(`key "%s" not found`, key))
		}
		ids = append(ids, cast.ToString(v))
	}

	result := d.parseSlice(ids)
	return result
}

func (d *branchesDialog) typeBranchesList() model.AllowedBranches {
	// Print first 10 branches for inspiration
	end := math.Min(10, float64(len(d.allBranches)))
	d.prompt.Printf("\nExisting project's branches, for inspiration:\n")
	for _, branch := range d.allBranches[:int(end)] {
		d.prompt.Printf("[%d] %s\n", branch.Id, branch.Name)
	}
	if len(d.allBranches) > 10 {
		d.prompt.Printf(`...`)
	}

	// Prompt
	lines, ok := d.prompt.Multiline(&prompt.Question{
		Label: "Allowed project's branches:",
		Description: "\nPlease enter one branch definition per line.\n" +
			"Each definition can be:\n" +
			"- branch ID\n" +
			"- branch name, with optional wildcards, eg. \"Foo Bar\", \"Dev:*\"\n" +
			"- branch directory (normalized) name, with optional wildcards, eg. \"foo-bar\", \"dev-*\"\n",
	})
	if !ok {
		return nil
	}

	// Normalize
	results := d.parseString(lines, "\n")
	return results
}

func (d *branchesDialog) parseString(str, sep string) model.AllowedBranches {
	return d.parseSlice(strings.Split(str, sep))
}

func (d *branchesDialog) parseSlice(items []string) model.AllowedBranches {
	branches := model.AllowedBranches{}
	for _, item := range items {
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
