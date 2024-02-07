package dbt

type Flags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"storage API host, eg. \"connection.keboola.com\""`
	TargetName     string `mapstructure:"target-name" usage:"target name of the profile"`
	WorkspaceName  string `mapstructure:"workspace-name" usage:"name of workspace to create"`
}
