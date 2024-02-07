package file

type DownloadFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"if command is run outside the project directory"`
	Output         string `mapstructure:"output" usage:"path to the destination file or directory"`
	AllowSliced    bool   `mapstructure:"allow-sliced" usage:"output sliced files as a directory containing slices as individual files"`
}

type UploadFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"if command is run outside the project directory"`
	Data           string `mapstructure:"data" usage:"path to the file to be uploaded"`
	FileName       string `mapstructure:"file-name" usage:"name of the file to be created"`
	FileTags       string `mapstructure:"file-tags" usage:"comma-separated list of tags"`
}
