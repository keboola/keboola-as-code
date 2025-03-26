package model

import (
	"slices"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	AllBranchesDef = "__all__"
	MainBranchDef  = "__main__"
)

const (
	IgnoredByAllowedKeys IgnoreReason = iota
	IgnoredByAllowedBranches
	IgnoredByIgnoredComponents
	IgnoredByAlwaysIgnoredComponents
)

type IgnoreReason int

type AllowedBranch string

type AllowedBranches []AllowedBranch

type ComponentIDs []keboola.ComponentID

// ObjectsFilter filters objects by allowed keys, allowed branches and ignored components.
type ObjectsFilter struct {
	allowedKeys       map[string]bool
	allowedBranches   AllowedBranches
	ignoredComponents ComponentIDs
}

type ObjectIsIgnoredError struct {
	error
	reason IgnoreReason
}

// nolint: gochecknoglobals
var alwaysIgnoredComponents = map[string]bool{
	keboola.WorkspacesComponent: true,
}

func DefaultAllowedBranches() AllowedBranches {
	return AllowedBranches{"*"}
}

func NewFilter(branches AllowedBranches, ignoredComponents ComponentIDs) ObjectsFilter {
	return ObjectsFilter{
		allowedBranches:   branches,
		ignoredComponents: ignoredComponents,
	}
}

func NoFilter() ObjectsFilter {
	return ObjectsFilter{
		allowedBranches:   DefaultAllowedBranches(),
		ignoredComponents: ComponentIDs{},
	}
}

func (f ObjectsFilter) IsObjectIgnored(object Object) bool {
	return f.AssertObjectAllowed(object) != nil
}

func (f ObjectsFilter) AssertObjectAllowed(object Object) *ObjectIsIgnoredError {
	if len(f.allowedKeys) > 0 {
		if !f.allowedKeys[object.Key().String()] {
			// Object key is not allowed -> object is ignored
			return objectIsIgnoredErrorf(IgnoredByAllowedKeys, `%s is ignored`, object.Desc())
		}
	}

	switch o := object.(type) {
	case *Branch:
		if !f.allowedBranches.IsBranchAllowed(o) {
			return objectIsIgnoredErrorf(IgnoredByAllowedBranches, `%s is ignored`, object.Desc())
		}
	case *Config:
		if slices.Contains(f.ignoredComponents, o.ComponentID) {
			return objectIsIgnoredErrorf(IgnoredByIgnoredComponents, `%s is ignored`, object.Desc())
		}
		if alwaysIgnoredComponents[o.ComponentID.String()] {
			return objectIsIgnoredErrorf(IgnoredByAlwaysIgnoredComponents, `%s is ignored, the component cannot be configured using a definition`, object.Desc())
		}
	case *ConfigWithRows:
		if slices.Contains(f.ignoredComponents, o.ComponentID) {
			return objectIsIgnoredErrorf(IgnoredByIgnoredComponents, `%s is ignored`, object.Desc())
		}
		if alwaysIgnoredComponents[o.ComponentID.String()] {
			return objectIsIgnoredErrorf(IgnoredByAlwaysIgnoredComponents, `%s is ignored, the component cannot be configured using a definition`, object.Desc())
		}
	case *ConfigRow:
		if slices.Contains(f.ignoredComponents, o.ComponentID) {
			return objectIsIgnoredErrorf(IgnoredByIgnoredComponents, `%s is ignored`, object.Desc())
		}
		if alwaysIgnoredComponents[o.ComponentID.String()] {
			return objectIsIgnoredErrorf(IgnoredByAlwaysIgnoredComponents, `%s is ignored, the component cannot be configured using a definition`, object.Desc())
		}
	}
	return nil
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

func (f *ObjectsFilter) IgnoredComponents() ComponentIDs {
	return f.ignoredComponents
}

func (f *ObjectsFilter) SetIgnoredComponents(ids ComponentIDs) {
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

func (v AllowedBranches) IsOneSpecificBranch() bool {
	// There is different number of definitions that one
	if len(v) != 1 {
		return false
	}

	branch := v[0]

	// All branches are allowed
	if branch == AllBranchesDef {
		return false
	}

	// Branch is defined via a wildcard
	if strings.ContainsAny(string(branch), "*?") {
		return false
	}

	return true
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
	if cast.ToInt(pattern) == int(branch.ID) {
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

func (v ComponentIDs) String() string {
	if len(v) == 0 {
		return `[]`
	}

	items := make([]string, 0)
	for _, item := range v {
		items = append(items, string(item))
	}
	return `"` + strings.Join(items, `", "`) + `"`
}

func (v ObjectIsIgnoredError) Reason() IgnoreReason {
	return v.reason
}

func objectIsIgnoredErrorf(reason IgnoreReason, format string, a ...any) *ObjectIsIgnoredError {
	return &ObjectIsIgnoredError{error: errors.Errorf(format, a...), reason: reason}
}
