package generate

type EnvFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
	WorkspaceID    string `mapstructure:"workspace-id" shorthand:"W" usage:"id of the workspace to use"`
}

type ProfileFlag struct {
	TargetName string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
}

type SourceFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
}
