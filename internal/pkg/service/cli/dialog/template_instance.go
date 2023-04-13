package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
)

// AskTemplateInstance - dialog to select template instance.
func (p *Dialogs) AskTemplateInstance(projectState *project.State) (branchKey model.BranchKey, instance *model.TemplateInstance, err error) {
	// Branch
	branch, err := p.SelectBranch(projectState.LocalObjects().Branches(), `Select branch`)
	if err != nil {
		return branchKey, instance, err
	}

	// Template instance
	instance, err = p.selectTemplateInstance(branch, `Select template instance`)
	return branch.BranchKey, instance, err
}
