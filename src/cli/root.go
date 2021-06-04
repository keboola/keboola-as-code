package cli

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"io"
	"keboola-as-code/src/log"
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

// flags from command line
type flags struct {
	workingDirectory string
	logFilePath      string
	verbose          bool
}

type rootCommand struct {
	cmd              *cobra.Command
	flags            *flags
	initialized      bool               // init method was called
	workingDirectory string             // working directory, can be specified by flag
	logFile          *os.File           // log file instance
	logFilePath      string             // log file path specified by flag, or generated temp file, or empty string if no log file
	logFileClear     bool               // is log file temporary? if yes, it will be removed at the end, if no error occurs
	logger           *zap.SugaredLogger // log to console and logFile
}

// NewRootCommand creates parent of all sub-commands
func NewRootCommand(stdout io.Writer, stderr io.Writer) *rootCommand {
	root := &rootCommand{flags: &flags{}}
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

	// Flags
	root.cmd.PersistentFlags().SortFlags = true
	root.cmd.PersistentFlags().BoolP("help", "h", false, "print help for command")
	root.cmd.PersistentFlags().StringVarP(&root.flags.workingDirectory, "dir", "d", "", "use other working directory")
	root.cmd.PersistentFlags().StringVarP(&root.flags.logFilePath, "log-file", "l", "", "path to a log file for details")
	root.cmd.PersistentFlags().BoolVarP(&root.flags.verbose, "verbose", "v", false, "print details")

	// Init when flags are parsed
	root.cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		root.init()
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
		root.init() // init, it can be uninitialized, if error occurred before PersistentPreRun call

		// Error is already logged to STDOUT, log to file
		root.logger.Debug("Command exited with error: ", err)
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

// init sets logger and working directory, must be called after flags are parsed
func (root *rootCommand) init() {
	if root.initialized {
		return
	}

	root.setupLogger()
	root.setupWorkingDirectory()
	root.logVersion()
	root.logCommand()
	root.initialized = true
}

// setupLogger according to the flags
func (root *rootCommand) setupLogger() {
	logFile, logFileErr := root.getLogFile()
	root.logger = log.NewLogger(root.cmd.OutOrStdout(), root.cmd.ErrOrStderr(), logFile, root.flags.verbose)
	root.logFile = logFile
	root.cmd.SetOut(log.ToInfoWriter(root.logger))
	root.cmd.SetErr(log.ToWarnWriter(root.logger))

	// Warn if user specified log file and it cannot be opened
	if logFileErr != nil && !root.logFileClear {
		root.logger.Warnf("Cannot open log file: %s", logFileErr)
	}
}

// setupWorkingDirectory from flag or current working directory
func (root *rootCommand) setupWorkingDirectory() {
	if len(root.flags.workingDirectory) > 0 {
		root.workingDirectory = root.flags.workingDirectory
	} else {
		dir, err := os.Getwd()
		if err != nil {
			panic(fmt.Errorf("cannot get current working directory: %s", err))
		}
		root.workingDirectory = dir
	}
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
	if len(root.flags.logFilePath) > 0 {
		root.logFilePath = root.flags.logFilePath
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
