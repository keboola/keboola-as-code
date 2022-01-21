package model

import (
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	AllBranchesDef = "__all__"
	MainBranchDef  = "__main__"
)

type AllowedBranch string

type AllowedBranches []AllowedBranch

type ComponentIds []ComponentId

// ObjectsFilter filters objects by allowed keys, allowed branches and ignored components.
type ObjectsFilter struct {
	allowedKeys       map[string]bool
	allowedBranches   AllowedBranches
	ignoredComponents ComponentIds
}

func DefaultAllowedBranches() AllowedBranches {
	return AllowedBranches{"*"}
}

func NewFilter(branches AllowedBranches, ignoredComponents ComponentIds) ObjectsFilter {
	return ObjectsFilter{
		allowedBranches:   branches,
		ignoredComponents: ignoredComponents,
	}
}

func NoFilter() ObjectsFilter {
	return ObjectsFilter{
		allowedBranches:   DefaultAllowedBranches(),
		ignoredComponents: ComponentIds{},
	}
}

func (f ObjectsFilter) IsObjectIgnored(object Object) bool {
	if len(f.allowedKeys) > 0 {
		if !f.allowedKeys[object.Key().String()] {
			// Object key is not allowed -> object is ignored
			return true
		}
	}

	switch o := object.(type) {
	case *Branch:
		return !f.allowedBranches.IsBranchAllowed(o)
	case *Config:
		return f.ignoredComponents.Contains(o.ComponentId)
	case *ConfigRow:
		return f.ignoredComponents.Contains(o.ComponentId)
	}
	return false
}

func (f *ObjectsFilter) SetAllowedKeys(keys []Key) {
	f.allowedKeys = make(map[string]bool)
	for _, key := range keys {
		f.allowedKeys[key.String()] = true
	}
}

func (f *ObjectsFilter) AllowedBranches() AllowedBranches {
	return f.allowedBranches
}

func (f *ObjectsFilter) SetAllowedBranches(branches AllowedBranches) {
	f.allowedBranches = branches
}

func (f *ObjectsFilter) IgnoredComponents() ComponentIds {
	return f.ignoredComponents
}

func (f *ObjectsFilter) SetIgnoredComponents(ids ComponentIds) {
	f.ignoredComponents = ids
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
	if cast.ToInt(pattern) == int(branch.Id) {
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

func (v ComponentIds) Contains(componentId ComponentId) bool {
	for _, id := range v {
		if id == componentId {
			return true
		}
	}
	return false
}
