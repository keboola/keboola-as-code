package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func (p *Dialogs) AskWorkflowsOptions() workflowsGen.Options {
	// Default values + values for non-interactive terminal
	out := workflowsGen.Options{
		Validate:   p.options.GetBool("ci-validate"),
		Push:       p.options.GetBool("ci-push"),
		Pull:       p.options.GetBool("ci-pull"),
		MainBranch: p.options.GetString("ci-main-branch"),
	}

	// Asl
	p.Printf("\nPlease confirm GitHub Actions you want to generate.")
	if !p.options.IsSet("ci-validate") {
		out.Validate = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"validate\" workflow?\nAll GitHub branches will be validated on change.",
			Default: out.Validate,
		})
	}
	if !p.options.IsSet("ci-push") {
		out.Push = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"push\" workflow?\nEach change in the main GitHub branch will be pushed to the project.",
			Default: out.Push,
		})
	}
	if !p.options.IsSet("ci-pull") {
		out.Pull = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"pull\" workflow?\nThe main GitHub branch will be synchronized each hour.\nIf a change found, then a new commit is created and pushed.",
			Default: out.Pull,
		})
	}
	if !p.options.IsSet("ci-main-branch") && (out.Push || out.Pull) {
		if mainBranch, ok := p.Select(&prompt.Select{
			Label:   "Please select the main GitHub branch name:",
			Options: []string{"main", "master"},
			Default: out.MainBranch,
		}); ok {
			out.MainBranch = mainBranch
		}
	}

	return out
}
