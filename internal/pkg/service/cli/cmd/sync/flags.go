package sync

type DiffFlag struct {
	Details bool `mapstructure:"details" usage:"print changed fields"`
}

type InitFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Branches       string `mapstructure:"branches" shorthand:"b" usage:"comma separated IDs or name globs, use \"*\" for all"`
	CI             bool   `mapstructure:"ci" usage:"generate workflows"`
	CIValidate     bool   `mapstructure:"ci-validate" usage:"create workflow to validate all branches on change"`
	CIPush         bool   `mapstructure:"ci-push" usage:"create workflow to push change in main branch to the project"`
	CIPull         bool   `mapstructure:"ci-pull" usage:"create workflow to sync main branch each hour"`
	CIMainBranch   string `mapstructure:"ci-main-branch" usage:"name of the main branch for push/pull workflows (default \"main\")"`
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
