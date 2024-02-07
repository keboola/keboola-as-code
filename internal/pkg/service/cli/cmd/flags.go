package cmd

type GlobalFlags struct {
	Help            bool   `mapstructure:"help" usage:"print help for command"`
	LogFile         string `mapstructure:"log-file" usage:"path to a log file for details"`
	LogFormat       string `mapstructure:"log-format" usage:"format of stdout and stderr"`
	NonInteractive  bool   `mapstructure:"non-interactive" usage:"disable interactive dialogs"`
	WorkingDir      string `mapstructure:"working-dir" usage:"use other working directory"`
	StorageAPIToken string `mapstructure:"storage-api-token" usage:"storage API token from your project"`
	Verbose         bool   `mapstructure:"verbose" usage:"print details"`
	VerboseAPI      bool   `mapstructure:"verbose-api" usage:"log each API request and response"`
	VersionCheck    bool   `mapstructure:"version-check" usage:"checks if there is a newer version of the CLI"`
}

func NewGlobalFlags() *GlobalFlags {
	return &GlobalFlags{
		VersionCheck: true,
		LogFormat:    "console",
	}
}
