package manifest

import (
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// LocalNaming of the files
type LocalNaming struct {
	Branch    utils.PathTemplate `json:"branch" validate:"required"`
	Config    utils.PathTemplate `json:"config" validate:"required"`
	ConfigRow utils.PathTemplate `json:"configRow" validate:"required"`
}

func DefaultNaming() *LocalNaming {
	return &LocalNaming{
		Branch:    "{branch_id}-{branch_name}",
		Config:    "{component_type}/{component_id}/{config_id}-{config_name}",
		ConfigRow: "rows/{config_row_id}-{config_row_name}",
	}
}

func (n *LocalNaming) BranchPath(branch *model.Branch) string {
	return utils.ReplacePlaceholders(string(n.Branch), map[string]interface{}{
		"branch_id":   branch.Id,
		"branch_name": utils.NormalizeName(branch.Name),
	})
}

func (n *LocalNaming) ConfigPath(component *model.Component, config *model.Config) string {
	return utils.ReplacePlaceholders(string(n.Config), map[string]interface{}{
		"component_type": component.Type,
		"component_id":   component.Id,
		"config_id":      config.Id,
		"config_name":    utils.NormalizeName(config.Name),
	})
}

func (n *LocalNaming) ConfigRowPath(row *model.ConfigRow) string {
	return utils.ReplacePlaceholders(string(n.ConfigRow), map[string]interface{}{
		"config_row_id":   row.Id,
		"config_row_name": utils.NormalizeName(row.Name),
	})
}
