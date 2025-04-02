package upgrade

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	upgradeTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/upgrade"
)

type upgradeTmplDialog struct {
	*dialog.Dialogs
	groups     input.StepsGroupsExt
	out        upgradeTemplate.Options
	inputsFile configmap.Value[string]
}

type upgradeTmplDeps interface {
	Logger() log.Logger
}

// AskUpgradeTemplateOptions - dialog for updating a template to a new version.
func AskUpgradeTemplateOptions(ctx context.Context, d *dialog.Dialogs, deps upgradeTmplDeps, projectState *state.State, branchKey model.BranchKey, instance model.TemplateInstance, groups template.StepsGroups, inputsFile configmap.Value[string]) (upgradeTemplate.Options, error) {
	groupsExt := upgrade.ExportInputsValues(ctx, deps.Logger().Debugf, projectState, branchKey, instance.InstanceID, groups)
	dialog := &upgradeTmplDialog{Dialogs: d, groups: groupsExt, inputsFile: inputsFile}
	dialog.out.Branch = branchKey
	dialog.out.Instance = instance
	return dialog.ask(ctx)
}

func (d *upgradeTmplDialog) ask(ctx context.Context) (upgradeTemplate.Options, error) {
	// User inputs
	if v, _, err := d.AskUseTemplateInputs(ctx, d.groups, false, d.inputsFile); err != nil {
		return d.out, err
	} else {
		d.out.Inputs = v
	}

	return d.out, nil
}
