package dialog

import (
	"fmt"

	"github.com/keboola/go-client/pkg/sandboxesapi"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
)

func (p *Dialogs) AskWorkspace(
	d *options.Options,
	allWorkspaces []*sandboxesapi.SandboxWithConfig,
) (*sandboxesapi.SandboxWithConfig, error) {
	if d.IsSet(`workspace-id`) {
		workspaceID := d.GetString(`workspace-id`)
		for _, w := range allWorkspaces {
			if string(w.Sandbox.ID) == workspaceID {
				return w, nil
			}
		}
		return nil, fmt.Errorf(`workspace with ID "%s" not found in the project`, workspaceID)
	}

	selectOpts := make([]string, 0)
	for _, w := range allWorkspaces {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, w.Config.Name, w.Sandbox.ID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "Workspace",
		Options: selectOpts,
	}); ok {
		return allWorkspaces[index], nil
	}

	return nil, fmt.Errorf(`please specify workspace`)
}
