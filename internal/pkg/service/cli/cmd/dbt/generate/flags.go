package generate

type EnvFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" usage:"target name of the profile"`
	WorkspaceID    string `mapstructure:"workspace-id" usage:"id of the workspace to use"`
}

type ProfileFlag struct {
	TargetName string `mapstructure:"target-name" usage:"target name of the profile"`
}

type SourceFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" usage:"target name of the profile"`
}
