package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func (p *Dialogs) AskWorkflowsOptions(options *options.Options) workflowsGen.Options {
	// Default values + values for non-interactive terminal
	out := workflowsGen.Options{
		Validate:   options.GetBool("ci-validate"),
		Push:       options.GetBool("ci-push"),
		Pull:       options.GetBool("ci-pull"),
		MainBranch: options.GetString("ci-main-branch"),
	}

	// Asl
	p.Printf("\nPlease confirm GitHub Actions you want to generate.")
	if !options.IsSet("ci-validate") {
		out.Validate = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"validate\" workflow?\nAll GitHub branches will be validated on change.",
			Default: out.Validate,
		})
	}
	if !options.IsSet("ci-push") {
		out.Push = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"push\" workflow?\nEach change in the main GitHub branch will be pushed to the project.",
			Default: out.Push,
		})
	}
	if !options.IsSet("ci-pull") {
		out.Pull = p.Confirm(&prompt.Confirm{
			Label:   "Generate \"pull\" workflow?\nThe main GitHub branch will be synchronized every 5 minutes.\nIf a change found, then a new commit is created and pushed.",
			Default: out.Pull,
		})
	}
	if !options.IsSet("ci-main-branch") && (out.Push || out.Pull) {
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
