package sync

import "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci"

type DiffFlag struct {
	Details bool `mapstructure:"details" usage:"print changed fields"`
}

type InitFlags struct {
	*ci.WorkflowFlags
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"storage API host, eg. \"connection.keboola.com\""`
	Branches       string `mapstructure:"branches" usage:"comma separated IDs or name globs, use \"*\" for all"`
}

type PullFlags struct {
	Force  bool `mapstructure:"force" usage:"ignore invalid local state"`
	DryRun bool `mapstructure:"dry-run" usage:"print what needs to be done"`
}

type PushFlags struct {
	Force   bool `mapstructure:"force" usage:"enable deleting of remote objects"`
	DryRun  bool `mapstructure:"dry-run" usage:"print what needs to be done"`
	Encrypt bool `mapstructure:"encrypt" usage:"encrypt unencrypted values before push"`
}

func NewInitFlags() *InitFlags {
	return &InitFlags{
		WorkflowFlags: ci.NewWorkflowFlags(),
		Branches:      "main",
	}
}
