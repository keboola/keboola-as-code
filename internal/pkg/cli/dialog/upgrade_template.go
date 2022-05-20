package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
)

type upgradeTmplDialog struct {
	*Dialogs
	projectState *project.State
	inputs       template.StepsGroups
	options      *options.Options
	out          upgradeTemplate.Options
}

// AskUpgradeTemplateOptions - dialog for deleting a template from the project.
func (p *Dialogs) AskUpgradeTemplateOptions(projectState *project.State, inputs template.StepsGroups, opts *options.Options) (upgradeTemplate.Options, error) {
	dialog := &upgradeTmplDialog{
		Dialogs:      p,
		projectState: projectState,
		inputs:       inputs,
		options:      opts,
	}
	return dialog.ask()
}

func (d *upgradeTmplDialog) ask() (upgradeTemplate.Options, error) {
	// Branch
	branch, err := d.SelectBranch(d.options, d.projectState.LocalObjects().Branches(), `Select branch`)
	if err != nil {
		return d.out, err
	}
	d.out.Branch = branch.BranchKey

	// Template instance
	instance, err := d.selectTemplateInstance(d.options, branch, `Select template instance`)
	if err != nil {
		return d.out, err
	}
	d.out.Instance = instance.InstanceId

	// User inputs
	if v, err := d.askUseTemplateInputs(d.inputs, d.options); err != nil {
		return d.out, err
	} else {
		d.out.Inputs = v
	}

	return d.out, nil
}
