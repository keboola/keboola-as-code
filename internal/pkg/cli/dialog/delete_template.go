package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	deleteTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
)

type deleteTmplDialog struct {
	*Dialogs
	projectState *project.State
	options      *options.Options
	out          deleteTemplate.Options
}

// AskDeleteTemplateOptions - dialog for deleting a template from the project.
func (p *Dialogs) AskDeleteTemplateOptions(projectState *project.State, opts *options.Options) (deleteTemplate.Options, error) {
	dialog := &deleteTmplDialog{
		Dialogs:      p,
		projectState: projectState,
		options:      opts,
	}

	return dialog.ask()
}

func (d *deleteTmplDialog) ask() (deleteTemplate.Options, error) {
	// Branch
	branch, err := d.SelectBranch(d.options, d.projectState.LocalObjects().Branches(), `Select branch`)
	if err != nil {
		return d.out, err
	}
	d.out.Branch = branch.BranchKey

	// Template instance
	instance, err := d.SelectTemplateInstance(d.options, branch, `Select template instance`)
	if err != nil {
		return d.out, err
	}
	d.out.Instance = instance.InstanceId

	return d.out, nil
}
