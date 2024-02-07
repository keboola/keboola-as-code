package ci

type WorkflowFlags struct {
	CI           bool   `mapstructure:"ci" usage:"generate workflows"`
	CIValidate   bool   `mapstructure:"ci-validate" usage:"create workflow to validate all branches on change"`
	CIPush       bool   `mapstructure:"ci-push" usage:"create workflow to push change in main branch to the project"`
	CIPull       bool   `mapstructure:"ci-pull" usage:"create workflow to sync main branch each hour"`
	CIMainBranch string `mapstructure:"ci-main-branch" usage:"name of the main branch for push/pull workflows"`
}

func NewWorkflowFlags() *WorkflowFlags {
	return &WorkflowFlags{
		CI:           true,
		CIValidate:   true,
		CIPush:       true,
		CIPull:       true,
		CIMainBranch: "main",
	}
}
