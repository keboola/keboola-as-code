package workflow

import (
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func AskWorkflowsOptions(flags Flags, d *dialog.Dialogs) workflowsGen.Options {
	// Default values + values for non-interactive terminal
	out := workflowsGen.Options{
		Validate:   flags.CIValidate.Value,
		Push:       flags.CIPush.Value,
		Pull:       flags.CIPull.Value,
		MainBranch: flags.CIMainBranch.Value,
	}

	// Asl
	d.Printf("\nPlease confirm GitHub Actions you want to generate.")
	if !flags.CIValidate.IsSet() {
		out.Validate = d.Confirm(&prompt.Confirm{
			Label:   "Generate \"validate\" workflow?\nAll GitHub branches will be validated on change.",
			Default: out.Validate,
		})
	}
	if !flags.CIPush.IsSet() {
		fmt.Println("dsadas")
		out.Push = d.Confirm(&prompt.Confirm{
			Label:   "Generate \"push\" workflow?\nEach change in the main GitHub branch will be pushed to the project.",
			Default: out.Push,
		})
	}
	if !flags.CIPull.IsSet() {
		out.Pull = d.Confirm(&prompt.Confirm{
			Label:   "Generate \"pull\" workflow?\nThe main GitHub branch will be synchronized each hour.\nIf a change found, then a new commit is created and pushed.",
			Default: out.Pull,
		})
	}
	if !flags.CIMainBranch.IsSet() && (out.Push || out.Pull) {
		if mainBranch, ok := d.Select(&prompt.Select{
			Label:   "Please select the main GitHub branch name:",
			Options: []string{"main", "master"},
			Default: out.MainBranch,
		}); ok {
			out.MainBranch = mainBranch
		}
	}

	return out
}
