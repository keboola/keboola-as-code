package workspace

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// WorkspaceWithConfig pairs a workspace instance with its keboola.sandboxes component config.
// For Python/R workspaces, App is set and Session is nil.
// For SQL (Snowflake/BigQuery) workspaces created via editor sessions, Session is set and App is nil.
type WorkspaceWithConfig struct {
	App     *keboola.DataScienceApp // non-nil for Python/R
	Session *keboola.EditorSession  // non-nil for SQL
	Config  *keboola.Config         // always set
}

func (w *WorkspaceWithConfig) Type() keboola.SandboxWorkspaceType {
	if w.App != nil {
		return keboola.SandboxWorkspaceType(w.App.Type)
	}
	if w.Session != nil {
		return keboola.SandboxWorkspaceType(w.Session.BackendType)
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
	return keboola.SandboxWorkspaceSupportsSizes(w.Type())
}

func (w *WorkspaceWithConfig) String() string {
	if w.SupportsSizes() {
		return fmt.Sprintf("ID: %s, Type: %s, Size: %s, Name: %s",
			w.Config.ID, w.Type(), w.Size(), w.Config.Name)
	}
	return fmt.Sprintf("ID: %s, Type: %s, Name: %s",
		w.Config.ID, w.Type(), w.Config.Name)
}
