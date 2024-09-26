package flag

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

type GlobalFlags struct {
	Help           configmap.Value[bool]   `configKey:"help" configShorthand:"h" configUsage:"print help for command"`
	LogFile        configmap.Value[string] `configKey:"log-file" configShorthand:"l" configUsage:"path to a log file for details"`
	LogFormat      configmap.Value[string] `configKey:"log-format" configUsage:"format of stdout and stderr"`
	NonInteractive configmap.Value[bool]   `configKey:"non-interactive" configUsage:"disable interactive dialogs"`
	WorkingDir     configmap.Value[string] `configKey:"working-dir" configShorthand:"d" configUsage:"use other working directory"`
	Verbose        configmap.Value[bool]   `configKey:"verbose" configShorthand:"v" configUsage:"print details"`
	VerboseAPI     configmap.Value[bool]   `configKey:"verbose-api" configUsage:"log each API request and response"`
	VersionCheck   configmap.Value[bool]   `configKey:"version-check" configUsage:"checks if there is a newer version of the CLI"`
}

func DefaultGlobalFlags() GlobalFlags {
	return GlobalFlags{
		VersionCheck: configmap.NewValueWithOrigin(true, configmap.SetByDefault),
		LogFormat:    configmap.NewValueWithOrigin("console", configmap.SetByDefault),
	}
}
