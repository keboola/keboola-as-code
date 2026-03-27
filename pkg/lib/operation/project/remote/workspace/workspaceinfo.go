package workspace

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// WorkspaceType is a string alias for the workspace type (snowflake, bigquery, python, r).
// Defined locally because the SDK no longer provides a combined type for all workspace backends.
type WorkspaceType = string

const (
	WorkspaceTypeSnowflake WorkspaceType = "snowflake"
	WorkspaceTypeBigQuery  WorkspaceType = "bigquery"
	WorkspaceTypePython    WorkspaceType = "python"
	WorkspaceTypeR         WorkspaceType = "r"
)

// WorkspaceSupportsSizes reports whether the given workspace type supports size selection.
func WorkspaceSupportsSizes(typ WorkspaceType) bool {
	return keboola.DataScienceSandboxSupportsSizes(keboola.DataScienceAppType(typ))
}

// WorkspaceTypesOrdered returns all workspace types in a stable display order.
func WorkspaceTypesOrdered() []WorkspaceType {
	return []WorkspaceType{WorkspaceTypeSnowflake, WorkspaceTypeBigQuery, WorkspaceTypePython, WorkspaceTypeR}
}

// WorkspaceTypesMap returns a set of all valid workspace types.
func WorkspaceTypesMap() map[WorkspaceType]bool {
	m := make(map[WorkspaceType]bool, len(WorkspaceTypesOrdered()))
	for _, t := range WorkspaceTypesOrdered() {
		m[t] = true
	}
	return m
}

// WorkspaceSizesOrdered returns sandbox sizes in ascending order.
func WorkspaceSizesOrdered() []string {
	sizes := keboola.DataScienceSandboxSizesOrdered()
	result := make([]string, len(sizes))
	for i, s := range sizes {
		result[i] = string(s)
	}
	return result
}

// WorkspaceSizesMap returns the set of valid sandbox sizes.
func WorkspaceSizesMap() map[string]bool {
	m := make(map[string]bool)
	for _, s := range keboola.DataScienceSandboxSizesOrdered() {
		m[string(s)] = true
	}
	return m
}

// WorkspaceWithConfig pairs a workspace instance with its keboola.sandboxes component config.
// For Python/R workspaces, App is set and Session is nil.
// For SQL (Snowflake/BigQuery) workspaces created via editor sessions, Session is set and App is nil.
type WorkspaceWithConfig struct {
	App     *keboola.DataScienceApp // non-nil for Python/R
	Session *keboola.EditorSession  // non-nil for SQL
	Config  *keboola.Config         // always set
}

func (w *WorkspaceWithConfig) Type() WorkspaceType {
	if w.App != nil {
		return string(w.App.Type)
	}
	if w.Session != nil {
		return string(w.Session.BackendType)
	}
	return ""
}

func (w *WorkspaceWithConfig) Size() string {
	if w.App != nil {
		return w.App.Size
	}
	return ""
}

func (w *WorkspaceWithConfig) SupportsSizes() bool {
	return WorkspaceSupportsSizes(w.Type())
}

func (w *WorkspaceWithConfig) String() string {
	if w.SupportsSizes() {
		return fmt.Sprintf("ID: %s, Type: %s, Size: %s, Name: %s",
			w.Config.ID, w.Type(), w.Size(), w.Config.Name)
	}
	return fmt.Sprintf("ID: %s, Type: %s, Name: %s",
		w.Config.ID, w.Type(), w.Config.Name)
}
