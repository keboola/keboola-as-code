package state

import (
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	AllBranchesDef = "__all__"
	MainBranchDef  = "__main__"
)

type Filter interface {
	IsObjectIgnored(object Object) bool
}

type AllowedBranch string

type AllowedBranches []AllowedBranch

// BaseFilter filters objects by allowed branches and ignored components.
type BaseFilter struct {
	allowedBranches   AllowedBranches
	ignoredComponents ComponentIds
}

// AllowedKeysFilter filters objects by allowed keys.
type AllowedKeysFilter struct {
	allowedKeys map[Key]bool
}

// composedFilter supports the composition of multiple filters.
type composedFilter struct {
	filters []Filter
}

func DefaultAllowedBranches() AllowedBranches {
	return AllowedBranches{"*"}
}

func NewBaseFilter() BaseFilter {
	return BaseFilter{
		allowedBranches: DefaultAllowedBranches(),
	}
}

func NewAllowedKeysFilter(keys ...Key) AllowedKeysFilter {
	f := AllowedKeysFilter{allowedKeys: make(map[Key]bool)}
	f.SetAllowedKeys(keys...)
	return f
}

func NewComposedFilter(filters ...Filter) Filter {
	return composedFilter{filters: filters}
}

func NewNoFilter() Filter {
	return NewComposedFilter()
}

func (f BaseFilter) IsObjectIgnored(object Object) bool {
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

func (f composedFilter) IsObjectIgnored(object Object) bool {
	for _, f := range f.filters {
		if f.IsObjectIgnored(object) {
			return true
		}
	}
	return false
}

func (f *BaseFilter) AllowedBranches() AllowedBranches {
	return f.allowedBranches
}

func (f *BaseFilter) SetAllowedBranches(branches AllowedBranches) {
	f.allowedBranches = branches
}

func (f *BaseFilter) IgnoredComponents() ComponentIds {
	return f.ignoredComponents
}

func (f *BaseFilter) SetIgnoredComponents(ids ComponentIds) {
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
	if cast.ToInt(pattern) == int(branch.BranchId) {
		return true
	}

	// Defined by name blob
	if match, _ := filesystem.Match(string(v), branch.Name); match {
		return true
	}

	// Defined by name blob - normalized name
	if match, _ := filesystem.Match(string(v), strhelper.NormalizeName(branch.Name)); match {
		return true
	}

	return false
}

func (f AllowedKeysFilter) IsObjectIgnored(object Object) bool {
	return !f.allowedKeys[object.Key()]
}

func (f *AllowedKeysFilter) SetAllowedKeys(keys ...Key) {
	f.allowedKeys = make(map[Key]bool)

	// Convert to a map for quick access
	for _, key := range keys {
		f.allowedKeys[key] = true
	}
}
