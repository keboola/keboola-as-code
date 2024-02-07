package job

type RunFlags struct {
	StorageAPIHost string `mapstructure:"storage-api-host" usage:"storage API host, eg. \"connection.keboola.com\""`
	Async          bool   `mapstructure:"async" usage:"do not wait for job to finish"`
	Timeout        string `mapstructure:"timeout" usage:"how long to wait for job to finish"`
}

func NewRunFlags() *RunFlags {
	return &RunFlags{
		Timeout: "5m",
	}
}
