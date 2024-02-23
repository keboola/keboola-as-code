package dialog

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AskTemplateInstance - dialog to select template instance.
func (p *Dialogs) AskTemplateInstance(projectState *project.State, branchName configmap.Value[string], instanceID configmap.Value[string]) (branchKey model.BranchKey, instance *model.TemplateInstance, err error) {
	// Branch
	branch, err := p.SelectBranch(projectState.LocalObjects().Branches(), `Select branch`, branchName)
	if err != nil {
		return branchKey, instance, err
	}

	// Template instance
	instance, err = p.selectTemplateInstance(branch, `Select template instance`, instanceID)
	return branch.BranchKey, instance, err
}

func (p *Dialogs) selectTemplateInstance(branch *model.Branch, label string, instanceID configmap.Value[string]) (*model.TemplateInstance, error) {
	if instanceID.IsSet() {
		usage, found, err := branch.Metadata.TemplateInstance(instanceID.Value)
		if err != nil {
			return nil, err
		}
		if found {
			return usage, nil
		}
		return nil, errors.Errorf(`template instance "%s" was not found in branch "%s"`, instanceID.Value, branch.Name)
	}

	all, err := branch.Metadata.TemplatesInstances()
	if err != nil {
		return nil, err
	}

	selectOpts := make([]string, 0)
	for _, u := range all {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s %s (%s)`, u.TemplateID, u.Version, u.InstanceID))
	}
	if index, ok := p.SelectIndex(&prompt.SelectIndex{
		Label:   label,
		Options: selectOpts,
	}); ok {
		return &all[index], nil
	}

	return nil, errors.New(`please specify template instance`)
}
