package workspace

type CreateFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	Name           string `mapstructure:"name" usage:"name of the workspace"`
	Type           string `mapstructure:"type" usage:"type of the workspace"`
	Size           string `mapstructure:"size" usage:"size of the workspace"`
}

type DeleteFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	WorkspaceID    string `mapstructure:"workspace-id" shorthand:"W" usage:"id of the workspace to delete"`
}

type DetailFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
	WorkspaceID    string `mapstructure:"workspace-id" shorthand:"W" usage:"id of the workspace to fetch"`
}

type ListFlag struct {
	StorageAPIHost string `mapstructure:"storage-api-host" shorthand:"H" usage:"storage API host, eg. \"connection.keboola.com\""`
}
