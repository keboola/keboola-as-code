package model

import (
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	AllBranchesDef       = "__all__"
	MainBranchDef        = "__main__"
	SchedulerComponentId = "keboola.scheduler"
)

type Filter struct {
	AllowedBranches   AllowedBranches `json:"allowedBranches" validate:"required,min=1"`
	IgnoredComponents ComponentIds    `json:"ignoredComponents"`
}

type AllowedBranch string
type AllowedBranches []AllowedBranch

type ComponentId string
type ComponentIds []ComponentId

func DefaultFilter() Filter {
	return Filter{
		AllowedBranches:   AllowedBranches{"*"},
		IgnoredComponents: ComponentIds{SchedulerComponentId},
	}
}

func (v AllowedBranches) String() string {
	if len(v) == 0 {
		return `[]`
	}

	items := make([]string, 0)
	for _, item := range v {
		items = append(items, string(item))
	}
	return `"` + strings.Join(items, `", "`) + `"`
}

func (v AllowedBranches) IsBranchAllowed(branch *Branch) bool {
	for _, definition := range v {
		if definition.IsBranchAllowed(branch) {
			return true
		}
	}
	return false
}

func (v AllowedBranch) IsBranchAllowed(branch *Branch) bool {
	pattern := string(v)

	// All branches
	if pattern == AllBranchesDef {
		return true
	}

	// Main branch
	if pattern == MainBranchDef && branch.IsDefault {
		return true
	}

	// Defined by ID
	if cast.ToInt(pattern) == branch.Id {
		return true
	}

	// Defined by name blob
	if match, _ := filesystem.Match(string(v), branch.Name); match {
		return true
	}

	// Defined by name blob - normalized name
	if match, _ := filesystem.Match(string(v), utils.NormalizeName(branch.Name)); match {
		return true
	}

	return false
}

func (v ComponentIds) String() string {
	if len(v) == 0 {
		return `[]`
	}

	items := make([]string, 0)
	for _, item := range v {
		items = append(items, string(item))
	}
	return `"` + strings.Join(items, `", "`) + `"`
}

func (v ComponentIds) Contains(componentId string) bool {
	for _, id := range v {
		if id == ComponentId(componentId) {
			return true
		}
	}
	return false
}
