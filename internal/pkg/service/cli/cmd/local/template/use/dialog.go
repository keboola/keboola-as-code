package use

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
)

type useTmplDialog struct {
	*dialog.Dialogs
	Flags
	projectState *project.State
	inputs       template.StepsGroups
	out          useTemplate.Options
}

// AskUseTemplateOptions - dialog for using the template in the project.
func AskUseTemplateOptions(ctx context.Context, d *dialog.Dialogs, projectState *project.State, inputs template.StepsGroups, f Flags) (useTemplate.Options, error) {
	dialog := &useTmplDialog{
		Dialogs:      d,
		projectState: projectState,
		inputs:       inputs,
		Flags:        f,
	}
	return dialog.ask(ctx)
}

func (d *useTmplDialog) ask(ctx context.Context) (useTemplate.Options, error) {
	// Target branch
	targetBranch, err := d.SelectBranch(d.projectState.LocalObjects().Branches(), `Select the target branch`, d.Branch)
	if err != nil {
		return d.out, err
	}
	d.out.TargetBranch = targetBranch.BranchKey

	// Instance name
	if v, err := d.askInstanceName(); err != nil {
		return d.out, err
	} else {
		d.out.InstanceName = v
	}

	// User inputs
	if v, _, err := d.AskUseTemplateInputs(ctx, d.inputs.ToExtended(), false, d.InputsFile); err != nil {
		return d.out, err
	} else {
		d.out.Inputs = v
	}

	return d.out, nil
}

func (d *useTmplDialog) askInstanceName() (string, error) {
	// Is flag set?
	if d.InstanceName.IsSet() {
		v := d.InstanceName.Value
		if len(v) > 0 {
			return v, nil
		}
	}

	// Ask for instance name
	v, _ := d.Ask(&prompt.Question{
		Label:       "Instance Name",
		Description: "Please enter an instance name to differentiate between multiple uses of the template.",
		Validator:   prompt.ValueRequired,
	})
	if len(v) == 0 {
		return "", errors.New(`please specify the instance name`)
	}
	return v, nil
}
