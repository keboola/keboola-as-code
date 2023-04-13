package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
)

// AskRenameInstance - dialog to rename template instance.
func (p *Dialogs) AskRenameInstance(projectState *project.State) (out renameOp.Options, err error) {
	// Select instance
	branchKey, instance, err := p.AskTemplateInstance(projectState)
	if err != nil {
		return out, err
	}
	out.Branch = branchKey
	out.Instance = *instance

	// Get name
	if v := p.options.GetString("new-name"); v != "" {
		out.NewName = v
	} else {
		// Ask for instance name
		v, _ := p.Prompt.Ask(&prompt.Question{
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
