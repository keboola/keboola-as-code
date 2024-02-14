package detail

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func AskWorkspaceID(d *dialog.Dialogs, f Flags) (string, error) {
	if !f.WorkspaceID.IsSet() {
		token, ok := d.Ask(&prompt.Question{
			Label:       "Workspace ID",
			Description: "Please enter the workspace ID",
			Validator:   prompt.ValueRequired,
		})
		if !ok {
			return "", errors.New("please specify workspace ID")
		}
		return token, nil
	} else {
		return f.WorkspaceID.Value, nil
	}
}
