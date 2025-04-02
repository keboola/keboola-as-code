package rename

import (
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
)

// AskRenameInstance - dialog to rename template instance.
func AskRenameInstance(projectState *project.State, d *dialog.Dialogs, f Flags) (out renameOp.Options, err error) {
	// Select instance
	branchKey, instance, err := d.AskTemplateInstance(projectState, f.Branch, f.Instance)
	if err != nil {
		return out, err
	}
	out.Branch = branchKey
	out.Instance = *instance

	// Get name
	if v := f.NewName.Value; v != "" {
		out.NewName = v
	} else {
		// Ask for instance name
		v, _ := d.Ask(&prompt.Question{
			Label:       "Instance Name",
			Description: "Please enter instance name.",
			Default:     instance.InstanceName,
			Validator:   prompt.ValueRequired,
		})
		out.NewName = v
	}

	if len(out.NewName) == 0 {
		return out, errors.New(`please specify the instance name`)
	}

	return out, nil
}
