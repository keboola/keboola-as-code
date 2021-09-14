package state

import (
	"strings"

	"keboola-as-code/src/fixtures"
	"keboola-as-code/src/utils"
)

// NewProjectSnapshot - to validate final project state in tests.
func NewProjectSnapshot(s *State) (*fixtures.ProjectSnapshot, error) {
	project := &fixtures.ProjectSnapshot{}

	branches := make(map[string]*fixtures.BranchConfigs)
	for _, bState := range s.Branches() {
		// Map branch
		branch := bState.Remote
		b := &fixtures.Branch{}
		b.Name = branch.Name
		b.Description = branch.Description
		b.IsDefault = branch.IsDefault
		branchConfigs := &fixtures.BranchConfigs{Branch: b, Configs: make([]*fixtures.Config, 0)}
		project.Branches = append(project.Branches, branchConfigs)
		branches[branch.String()] = branchConfigs
	}

	configs := make(map[string]*fixtures.Config)
	for _, cState := range s.Configs() {
		config := cState.Remote
		c := &fixtures.Config{Rows: make([]*fixtures.ConfigRow, 0)}
		c.ComponentId = config.ComponentId
		c.Name = config.Name
		c.Description = config.Description
		c.ChangeDescription = normalizeChangeDesc(config.ChangeDescription)
		c.Content = config.Content
		b := branches[config.BranchKey().String()]
		b.Configs = append(b.Configs, c)
		configs[config.String()] = c
	}

	for _, rState := range s.ConfigRows() {
		row := rState.Remote
		r := &fixtures.ConfigRow{}
		r.Name = row.Name
		r.Description = row.Description
		r.ChangeDescription = normalizeChangeDesc(row.ChangeDescription)
		r.IsDisabled = row.IsDisabled
		r.Content = row.Content
		c := configs[row.ConfigKey().String()]
		c.Rows = append(c.Rows, r)
	}

	// Sort by name
	utils.SortByName(project.Branches)
	for _, b := range project.Branches {
		utils.SortByName(b.Configs)
		for _, c := range b.Configs {
			utils.SortByName(c.Rows)
		}
	}

	return project, nil
}

func normalizeChangeDesc(str string) string {
	// Default description if object has been created by test
	if str == "created by test" {
		return ""
	}

	// Default description if object has been created with a new branch
	if strings.HasPrefix(str, "Copied from ") {
		return ""
	}
	// Default description if rows has been deleted
	if strings.HasSuffix(str, " deleted") {
		return ""
	}

	return str
}
