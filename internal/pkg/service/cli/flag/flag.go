package flag

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"

type GlobalFlags struct {
	Help            bool                    `configKey:"help" configShorthand:"h" configUsage:"print help for command"`
	LogFile         string                  `configKey:"log-file" configShorthand:"l" configUsage:"path to a log file for details"`
	LogFormat       string                  `configKey:"log-format" configUsage:"format of stdout and stderr"`
	NonInteractive  bool                    `configKey:"non-interactive" configUsage:"disable interactive dialogs"`
	WorkingDir      string                  `configKey:"working-dir" configShorthand:"d" configUsage:"use other working directory"`
	StorageAPIToken string                  `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API token from your project"`
	Verbose         bool                    `configKey:"verbose" configShorthand:"v" configUsage:"print details"`
	VerboseAPI      bool                    `configKey:"verbose-api" configUsage:"log each API request and response"`
	VersionCheck    bool                    `configKey:"version-check" configUsage:"checks if there is a newer version of the CLI"`
}

func DefaultGlobalFlags() GlobalFlags {
	return GlobalFlags{
		VersionCheck: true,
		LogFormat:    "console",
	}
}
