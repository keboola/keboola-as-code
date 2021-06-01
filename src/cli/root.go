package cli

import (
	"github.com/spf13/cobra"
	"os"
	"path"
)

const description =
`
Keboola Connection pull/push client
for components configurations.

Configurations can be synchronized in both
directions [KBC project] <-> [a local directory].
`

const usageTemplate =
`Usage:{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if gt (len .Aliases) 0}}

Aliases:
  {{.NameAndAliases}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}{{if .HasAvailableSubCommands}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.{{end}}
`

// rootCommand, parent of all sub-commands
func (c *commander) rootCommand() *cobra.Command{
	// Define command
	cmdName := path.Base(os.Args[0])
	cmd := &cobra.Command{
		Use:     cmdName,
		Version: c.version(),
		Short:   description,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Print help if no command specified
			return cmd.Help()
		},
	}
	cmd.SetVersionTemplate("{{.Version}}")
	cmd.SetUsageTemplate(usageTemplate)

	// Setup
	cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		c.init()
	}

	// Flags
	cmd.PersistentFlags().SortFlags = true
	cmd.PersistentFlags().BoolP("help", "h", false, "print help for command")
	cmd.PersistentFlags().StringVarP(&c.flags.workingDirectory, "dir", "d", "", "use other working directory")
	cmd.PersistentFlags().StringVarP(&c.flags.logFilePath, "log-file", "l", "", "path to a log file for details")
	cmd.PersistentFlags().BoolVarP(&c.flags.verbose, "verbose", "v", false, "print details")

	// Sub-commands
	cmd.AddCommand(
		c.initCommand(),
	)

	return cmd
}
