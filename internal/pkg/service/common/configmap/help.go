package configmap

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
)

type HelpError struct {
	Help string
}

func (h HelpError) Error() string {
	return "help requested"
}

func newHelpError(name string, flags *pflag.FlagSet, cfg BindSpec) HelpError {
	var b strings.Builder

	b.WriteString(fmt.Sprintf(`Usage of "%s":`, name))
	b.WriteString("\n")
	b.WriteString(flags.FlagUsages())

	if cfg.EnvNaming != nil && cfg.Envs != nil && cfg.GenerateConfigFileFlag {
		b.WriteString("\n")
		b.WriteString("Configuration source priority: 1. flag, 2. ENV, 3. config file\n")
	}

	if cfg.EnvNaming != nil && cfg.Envs != nil {
		b.WriteString("\n")
		b.WriteString("Flags can also be defined as ENV variables.\n")
		b.WriteString(fmt.Sprintf("For example, the flag \"--foo-bar\" becomes the \"%s\" ENV.\n", cfg.EnvNaming.FlagToEnv("foo-bar")))
	}

	if cfg.GenerateConfigFileFlag {
		b.WriteString("\n")
		b.WriteString(fmt.Sprintf("Use \"--%s\" flag to specify a JSON/YAML configuration file, it can be used multiple times.\n", ConfigFileFlag))
	}

	if cfg.GenerateDumpConfigFlag {
		b.WriteString("\n")
		b.WriteString(fmt.Sprintf("Use \"--%s\" flag with \"json\" or \"yaml\" value to dump configuration to STDOUT.\n", DumpConfigFlag))
	}

	b.WriteString("\n")

	return HelpError{Help: b.String()}
}
