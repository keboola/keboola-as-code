package create

type BranchFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"if command is run outside the project directory"`
	Name           string `mapstructure:"name" usage:"name of the new branch"`
}

type BucketFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"if command is run outside the project directory"`
	Description    string `mapstructure:"description" usage:"bucket description"`
	DisplayName    string `mapstructure:"display-name" usage:"display name for the UI"`
	Name           string `mapstructure:"name" usage:"name of the bucket"`
	Stage          string `mapstructure:"stage" usage:"stage, allowed values: in, out"`
}

type TableFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"if command is run outside the project directory"`
	Bucket         string `mapstructure:"bucket" usage:"bucket ID (required if the tableId argument is empty)"`
	Name           string `mapstructure:"name" usage:"name of the table (required if the tableId argument is empty)"`
	Columns        string `mapstructure:"columns" usage:"comma-separated list of column names"`
	PrimaryKey     string `mapstructure:"primary-key" usage:"columns used as primary key, comma-separated"`
}
