package dbt

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" shorthand:"T" usage:"target name of the profile"`
	WorkspaceName  string `mapstructure:"workspace-name" shorthand:"W" usage:"name of workspace to create"`
}
