package cli

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"io"
	"keboola-as-code/src/log"
	"keboola-as-code/src/params"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/version"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
)

const description = `
Keboola Connection pull/push client
for components configurations.

Configurations can be synchronized in both
directions [KBC project] <-> [a local directory].
`

const usageTemplate = `Usage:{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{else if .Runnable}}
  {{.UseLine}}{{end}}{{if gt (len .Aliases) 0}}

Aliases:`

type rootCommand struct {
	cmd          *cobra.Command
	params       *params.Params     // parsed Flags and env variables
	flags        *params.Flags      // values of command line Flags
	initialized  bool               // init method was called
	logFile      *os.File           // log file instance
	logFilePath  string             // log file path specified by flag, or generated temp file, or empty string if no log file
	logFileClear bool               // is log file temporary? if yes, it will be removed at the end, if no error occurs
	logger       *zap.SugaredLogger // log to console and logFile
}

// NewRootCommand creates parent of all sub-commands
func NewRootCommand(stdout io.Writer, stderr io.Writer) *rootCommand {
	root := &rootCommand{flags: &params.Flags{}}
	root.cmd = &cobra.Command{
		Use:          path.Base(os.Args[0]), // name of the binary
		Version:      version.Version(),
		Short:        description,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Print help if no command specified
			return root.cmd.Help()
		},
	}

	// Setup outputs
	root.cmd.SetOut(stdout)
	root.cmd.SetErr(stderr)

	// Setup templates
	root.cmd.SetVersionTemplate("{{.Version}}")
	root.cmd.SetUsageTemplate(
		regexp.MustCompile(`Usage:(.|\n)*Aliases:`).ReplaceAllString(root.cmd.UsageTemplate(), usageTemplate),
	)

	// Persistent flags
	root.cmd.PersistentFlags().SortFlags = true
	root.cmd.PersistentFlags().StringVarP(&root.flags.ApiUrl, "api-url", "u", "", "storage API url, eg. \"connection.keboola.com\"")
	root.cmd.PersistentFlags().StringVarP(&root.flags.WorkingDirectory, "dir", "d", "", "use other working directory")
	root.cmd.PersistentFlags().BoolP("help", "h", false, "print help for command")
	root.cmd.PersistentFlags().StringVarP(&root.flags.LogFilePath, "log-file", "l", "", "path to a log file for details")
	root.cmd.PersistentFlags().StringVarP(&root.flags.ApiToken, "token", "t", "", "storage API token")
	root.cmd.PersistentFlags().BoolVarP(&root.flags.Verbose, "verbose", "v", false, "print details")

	// Root flags
	root.cmd.Flags().SortFlags = true
	root.cmd.Flags().Bool("version", false, "print version")

	// Init when flags are parsed
	root.cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		return root.init()
	}

	// Sub-commands
	root.cmd.AddCommand(
		initCommand(root),
	)

	return root
}

// Execute command or sub-command
func (root *rootCommand) Execute() {
	defer root.tearDown()
	if err := root.cmd.Execute(); err != nil {
		// Init, it can be uninitialized, if error occurred before PersistentPreRun call
		_ = root.init()
		// Error is already logged
		os.Exit(1)
	}
}

func (root *rootCommand) GetCommandByName(name string) *cobra.Command {
	for _, cmd := range root.cmd.Commands() {
		if cmd.Name() == name {
			return cmd
		}
	}

	return nil
}

// tearDown makes clean-up after command execution
func (root *rootCommand) tearDown() {
	if err := recover(); err == nil {
		// No error -> remove log file if temporary
		if root.logFile != nil && root.logFileClear {
			if err = root.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", root.logFilePath, err))
			}
			if err = os.Remove(root.logFilePath); err != nil {
				panic(fmt.Errorf("cannot remove temp log file \"%s\": %s", root.logFilePath, err))
			}
		}
	} else {
		// Error -> process and close log file
		exitCode := utils.ProcessPanic(err, root.logger, root.logFilePath)
		if root.logFile != nil {
			if err = root.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", root.logFilePath, err))
			}
		}
		os.Exit(exitCode)
	}
}

// init sets logger and params after flags are parsed
func (root *rootCommand) init() (err error) {
	if root.initialized {
		return
	}

	root.setupLogger()
	root.logVersion()
	root.logCommand()

	err = root.setupParams()
	if err != nil {
		return
	}

	root.initialized = true
	return
}

// setupLogger according to the flags
func (root *rootCommand) setupLogger() {
	logFile, logFileErr := root.getLogFile()
	root.logger = log.NewLogger(root.cmd.OutOrStdout(), root.cmd.ErrOrStderr(), logFile, root.flags.Verbose)
	root.logFile = logFile
	root.cmd.SetOut(log.ToInfoWriter(root.logger))
	root.cmd.SetErr(log.ToWarnWriter(root.logger))

	// Warn if user specified log file and it cannot be opened
	if logFileErr != nil && !root.logFileClear {
		root.logger.Warnf("Cannot open log file: %s", logFileErr)
	}
}

func (root *rootCommand) setupParams() (err error) {
	root.params, err = params.NewParams(root.logger, root.flags)
	return
}

func (root *rootCommand) logVersion() {
	versionLines := bufio.NewScanner(strings.NewReader(root.cmd.Version))
	for versionLines.Scan() {
		root.logger.Debug(versionLines.Text())
	}
}

func (root *rootCommand) logCommand() {
	root.logger.Debugf("Running command %v", os.Args)
}

// Get log file defined in the flags or create a temp file
func (root *rootCommand) getLogFile() (logFile *os.File, logFileErr error) {
	if len(root.flags.LogFilePath) > 0 {
		root.logFilePath = root.flags.LogFilePath
		root.logFileClear = false // log file defined by user will be preserved
	} else {
		root.logFilePath = path.Join(os.TempDir(), fmt.Sprintf("keboola-as-code-%d.txt", time.Now().Unix()))
		root.logFileClear = true // temp log file will be removed. It will be preserved only in case of error
	}

	logFile, logFileErr = os.OpenFile(root.logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if logFileErr != nil {
		root.logFilePath = ""
	}
	return
}
