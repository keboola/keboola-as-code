package dialog

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskWorkspace(allWorkspaces []*keboola.WorkspaceWithConfig) (*keboola.WorkspaceWithConfig, error) {
	if p.options.IsSet(`workspace-id`) {
		workspaceID := p.options.GetString(`workspace-id`)
		for _, w := range allWorkspaces {
			if string(w.Config.ID) == workspaceID {
				return w, nil
			}
		}
		return nil, errors.Errorf(`workspace with ID "%s" not found in the project`, workspaceID)
	}

	selectOpts := make([]string, 0)
	for _, w := range allWorkspaces {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, w.Config.Name, w.Config.ID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   "Workspace",
		Options: selectOpts,
	}); ok {
		return allWorkspaces[index], nil
	}

	return nil, errors.New(`please specify workspace`)
}

func (p *Dialogs) AskWorkspaceID() (string, error) {
	if !p.options.IsSet(`workspace-id`) {
		token, ok := p.Ask(&prompt.Question{
			Label:       "Workspace ID",
			Description: "Please enter the workspace ID",
			Validator:   prompt.ValueRequired,
		})
		if !ok {
			return "", errors.New("please specify workspace ID")
		}
		return token, nil
	} else {
		return p.options.GetString(`workspace-id`), nil
	}
}
