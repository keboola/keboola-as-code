package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/upgrade"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
)

type upgradeTmplDialog struct {
	*Dialogs
	groups  input.StepsGroupsExt
	options *options.Options
	out     upgradeTemplate.Options
}

// AskUpgradeTemplateOptions - dialog for deleting a template from the project.
func (d *Dialogs) AskUpgradeTemplateOptions(projectState *project.State, branchKey model.BranchKey, instance model.TemplateInstance, groups template.StepsGroups, opts *options.Options) (upgradeTemplate.Options, error) {
	groupsExt := upgrade.ExportInputsValues(projectState.State(), branchKey, instance.InstanceId, groups)
	dialog := &upgradeTmplDialog{Dialogs: d, groups: groupsExt, options: opts}
	dialog.out.Branch = branchKey
	dialog.out.Instance = instance
	return dialog.ask()
}

func (d *upgradeTmplDialog) ask() (upgradeTemplate.Options, error) {
	// User inputs
	if v, err := d.askUseTemplateInputs(d.groups, d.options); err != nil {
		return d.out, err
	} else {
		d.out.Inputs = v
	}

	return d.out, nil
}
