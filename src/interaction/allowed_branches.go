package interaction

import (
	"fmt"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"math"
	"strings"

	"github.com/spf13/cast"
)

const (
	ModeMainBranch     = "only main branch"
	ModeAllBranches    = "all branches"
	ModeSelectSpecific = "select branches"
	ModeTypeList       = "type IDs or names"
)

type branchesPrompt struct {
	prompt      *Prompt
	allBranches []*model.Branch
	isFlagSet   bool
	flagValue   string
	defaultMode interface{}
}

func (p *Prompt) GetAllowedBranches(allBranches []*model.Branch, isFlagSet bool, flagValue string) model.AllowedBranches {
	// Convert CLI values to internal
	if flagValue == "*" {
		flagValue = model.AllBranchesDef
	} else if flagValue == "main" {
		flagValue = model.MainBranchDef
	}

	return (&branchesPrompt{
		prompt:      p,
		allBranches: allBranches,
		isFlagSet:   isFlagSet,
		flagValue:   flagValue,
	}).ask()
}

func (p *branchesPrompt) ask() model.AllowedBranches {
	if p.prompt.Interactive && !p.isFlagSet {
		for {
			results := p.doAsk()

			// Check that the definition meets at least one branch
			matched := 0
			for _, branch := range p.allBranches {
				for _, definition := range results {
					if definition.IsBranchAllowed(branch) {
						matched++
					}
				}
			}

			// If a branch is matched -> ok
			if matched > 0 {
				if matched == 1 {
					p.prompt.Printf("\nOne branch match \"allowed branches\". Ok.\n")
				} else {
					p.prompt.Printf("\n%d branches match \"allowed branches\". Ok.\n", matched)
				}
				return results
			}

			p.prompt.Printf("\nNo existing branch matches your definitions. Please try again.\n")
		}
	}

	// Parse flag value
	return p.parseString(p.flagValue, ",")
}

func (p *branchesPrompt) doAsk() model.AllowedBranches {
	// Ask user for mode
	mode, ok := p.prompt.Select(&Select{
		Label:       "Allowed branches",
		Description: "Please select which branches you want to use with this CLI tool.",
		Options: []string{
			ModeMainBranch,
			ModeAllBranches,
			ModeSelectSpecific,
			ModeTypeList,
		},
		Default: p.defaultMode,
	})

	// If it is necessary to repeat the selection (an error occurs), the option will be used as default
	p.defaultMode = mode

	// Load definitions according to the specified mode
	if ok {
		switch mode {
		case ModeMainBranch:
			return model.AllowedBranches{model.MainBranchDef}
		case ModeAllBranches:
			return model.AllowedBranches{model.AllBranchesDef}
		case ModeSelectSpecific:
			return p.selectBranches()
		case ModeTypeList:
			return p.typeBranchesList()
		}
	}

	return nil
}

func (p *branchesPrompt) selectBranches() model.AllowedBranches {
	// Build options
	options := utils.NewOrderedMap()
	for _, branch := range p.allBranches {
		msg := fmt.Sprintf(`[%d] %s`, branch.Id, branch.Name)
		options.Set(msg, branch.Id)
	}

	// Prompt
	keys, ok := p.prompt.MultiSelect(&Select{
		Label:       "Allowed branches multi-select",
		Description: "Please select one or more branches.",
		Options:     options.Keys(),
	})

	if !ok {
		return nil
	}

	ids := make([]string, 0)
	for _, key := range keys {
		v, found := options.Get(key)
		if !found {
			panic(fmt.Errorf(`key "%s" not found`, key))
		}
		ids = append(ids, cast.ToString(v))
	}

	result := p.parseSlice(ids)
	return result
}

func (p *branchesPrompt) typeBranchesList() model.AllowedBranches {
	// Print first 10 branches for inspiration
	end := math.Min(10, float64(len(p.allBranches)))
	p.prompt.Printf("\nExisting branches, for inspiration:\n")
	for _, branch := range p.allBranches[:int(end)] {
		p.prompt.Printf("[%d] %s\n", branch.Id, branch.Name)
	}
	if len(p.allBranches) > 10 {
		p.prompt.Printf(`...`)
	}

	// Prompt
	lines, ok := p.prompt.Multiline(&Question{
		Label: "Allowed branches definitions",
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
	results := p.parseString(lines, "\n")
	return results
}

func (p *branchesPrompt) parseString(str, sep string) model.AllowedBranches {
	return p.parseSlice(strings.Split(str, sep))
}

func (p *branchesPrompt) parseSlice(items []string) model.AllowedBranches {
	branches := model.AllowedBranches{}
	for _, item := range items {
		item = strings.TrimSpace(item)
		if len(item) == 0 {
			continue
		}
		branches = append(branches, model.AllowedBranch(item))
	}
	return p.unique(branches)
}

// unique returns only unique items
func (p *branchesPrompt) unique(items model.AllowedBranches) model.AllowedBranches {
	m := utils.NewOrderedMap()
	for _, item := range items {
		m.Set(string(item), true)
	}

	unique := model.AllowedBranches{}
	for _, item := range m.Keys() {
		unique = append(unique, model.AllowedBranch(item))
	}
	return unique
}
