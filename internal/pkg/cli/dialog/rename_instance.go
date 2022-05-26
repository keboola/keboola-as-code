package dialog

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
)

// AskRenameInstance - dialog to rename template instance.
func (d *Dialogs) AskRenameInstance(projectState *project.State, opts *options.Options) (out renameOp.Options, err error) {
	// Select instance
	branchKey, instance, err := d.AskTemplateInstance(projectState, opts)
	if err != nil {
		return out, err
	}
	out.Branch = branchKey
	out.Instance = *instance

	// Get name
	if v := opts.GetString("new-name"); v != "" {
		out.NewName = v
	} else {
		// Ask for instance name
		v, _ := d.Prompt.Ask(&prompt.Question{
			Label:       "Instance Name",
			Description: "Please enter instance name.",
			Default:     instance.InstanceName,
			Validator:   prompt.ValueRequired,
		})
		out.NewName = v
	}

	if len(out.NewName) == 0 {
		return out, fmt.Errorf(`please specify the instance name`)
	}

	return out, nil
}
