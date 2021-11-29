package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	versionCheck "github.com/keboola/keboola-as-code/pkg/lib/operation/remote/version/check"
)

const description = `
Keboola CLI

Manage your Keboola Connection project
from your local machine or CI pipeline.

Project can be synchronized in both
directions [Keboola Connection] <-> [local directory].

Start by running the "init" sub-command in a new empty directory.
Your project will be pulled and you can start working.
`

const usageTemplate = `Usage:{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{else if .Runnable}}
  {{.UseLine}}{{end}}{{if gt (len .Aliases) 0}}

Aliases:`

type RootCommand struct {
	Options *options.Options
	Logger  *zap.SugaredLogger
	Deps    *dependencies.Container
	cmd     *cobra.Command
	logFile *log.File
}

// NewRootCommand creates parent of all sub-commands.
func NewRootCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer, prompt prompt.Prompt, envs *env.Map, fsFactory filesystem.Factory) *RootCommand {
	// Command definition
	root := &RootCommand{Options: options.NewOptions()}
	root.cmd = &cobra.Command{
		Use:           path.Base(os.Args[0]), // name of the binary
		Version:       version.Version(),
		Short:         description,
		SilenceUsage:  true,
		SilenceErrors: true, // custom error handling, see printError
		RunE: func(cmd *cobra.Command, args []string) error {
			// Print help if no command specified
			return root.cmd.Help()
		},
	}

	// Setup in/out
	root.cmd.SetIn(stdin)
	root.cmd.SetOut(stdout)
	root.cmd.SetErr(stderr)

	// Setup templates
	root.cmd.SetVersionTemplate("{{.Version}}")
	root.cmd.SetUsageTemplate(
		regexp.MustCompile(`Usage:(.|\n)*Aliases:`).ReplaceAllString(root.cmd.UsageTemplate(), usageTemplate),
	)

	// Persistent flags for all sub-commands
	flags := root.cmd.PersistentFlags()
	flags.SortFlags = true
	flags.BoolP("help", "h", false, "print help for command")
	flags.StringP("log-file", "l", "", "path to a log file for details")
	flags.StringP("working-dir", "d", "", "use other working directory")
	flags.StringP("storage-api-token", "t", "", "storage API token from your project")
	flags.BoolP("verbose", "v", false, "print details")
	flags.BoolP("verbose-api", "", false, "log each API request and response")

	// Root command flags
	root.cmd.Flags().SortFlags = true
	root.cmd.Flags().BoolP("version", "V", false, "print version")

	// Init when flags are parsed
	root.cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Temporary logger
		tmpLogger := zap.NewNop().Sugar()

		// Create filesystem abstraction
		workingDir, _ := cmd.Flags().GetString(`working-dir`)
		fs, err := fsFactory(tmpLogger, workingDir)
		if err != nil {
			return err
		}

		// Load values from flags and envs
		if err = root.Options.Load(tmpLogger, envs, fs, cmd.Flags()); err != nil {
			return err
		}

		// Setup logger
		root.setupLogger()
		fs.SetLogger(root.Logger)
		root.Logger.Debug(`Working dir: `, filesystem.Join(fs.BasePath(), fs.WorkingDir()))

		// Create dependencies container
		root.Deps = dependencies.NewContainer(root.cmd.Context(), envs, fs, dialog.New(prompt), root.Logger, root.Options)

		// Check version
		if err := versionCheck.Run(root.Deps); err != nil {
			// Ignore error, send to logs
			root.Logger.Debugf(`Version check: %s.`, err.Error())
		}

		return nil
	}

	// Sub-commands
	root.cmd.AddCommand(
		StatusCommand(root),
		InitCommand(root),
		ValidateCommand(root),
		PullCommand(root),
		PushCommand(root),
		DiffCommand(root),
		PersistCommand(root),
		FixPathsCommand(root),
		EncryptCommand(root),
		WorkflowsCommand(root),
		CreateCommand(root),
	)

	return root
}

// Execute command or sub-command.
func (root *RootCommand) Execute() (exitCode int) {
	defer func() {
		exitCode = root.tearDown(exitCode)
	}()

	// Logger can be nil, if error occurred before initialization
	if root.Logger == nil {
		root.setupLogger()
	}

	if err := root.cmd.Execute(); err != nil {
		root.printError(err)
		return 1
	}
	return 0
}

func (root *RootCommand) printError(errRaw error) {
	// Convert to MultiError
	err := utils.NewMultiError()
	if !errors.As(errRaw, &err) {
		err.Append(errRaw)
	}

	// Iterate over errors and replace message if needed
	modifiedErrs := utils.NewMultiError()
	for _, err := range err.Errors {
		switch {
		case errors.Is(err, dependencies.ErrMetadataDirFound):
			root.Logger.Infof(`The path "%s" is already an project directory.`, root.Deps.Fs().BasePath())
			root.Logger.Info(`Please use a different directory or synchronize the current with "pull" command.`)
			modifiedErrs.Append(fmt.Errorf(`metadata directory "%s" already exists`, filesystem.MetadataDir))
		case errors.Is(err, dependencies.ErrMetadataDirNotFound):
			root.Logger.Infof(`Project directory must contain the ".keboola" metadata directory.`)
			root.Logger.Infof(`Please change working directory to a project directory or use the "init" command.`)
			modifiedErrs.Append(fmt.Errorf(`none of this and parent directories is project dir`))
		case errors.Is(err, dependencies.ErrMissingStorageApiHost):
			modifiedErrs.Append(fmt.Errorf(`- missing Storage Api host, please use "--%s" flag or ENV variable "%s"`, options.StorageApiHostOpt, root.Options.GetEnvName(options.StorageApiHostOpt)))
		case errors.Is(err, dependencies.ErrMissingStorageApiToken):
			modifiedErrs.Append(fmt.Errorf(`- missing Storage Api token, please use "--%s" flag or ENV variable "%s"`, options.StorageApiTokenOpt, root.Options.GetEnvName(options.StorageApiTokenOpt)))
		default:
			modifiedErrs.Append(err)
		}
	}

	root.cmd.PrintErrln(utils.PrefixError(`Error`, modifiedErrs))
}

func (root *RootCommand) setupLogger() {
	// Get log file
	var logFileErr error
	root.logFile, logFileErr = log.NewLogFile(root.Options.LogFilePath)

	// Create logger
	root.Logger = log.NewLogger(root.cmd.OutOrStdout(), root.cmd.ErrOrStderr(), root.logFile, root.Options.Verbose)
	root.cmd.SetOut(log.ToInfoWriter(root.Logger))
	root.cmd.SetErr(log.ToWarnWriter(root.Logger))

	// Warn if user specified log file + it cannot be opened
	if logFileErr != nil && root.Options.LogFilePath != "" {
		root.Logger.Warnf("Cannot open log file: %s", logFileErr)
	}

	// Log info
	w := log.ToDebugWriter(root.Logger)
	w.WriteStringNoErr(root.cmd.Version)
	w.WriteStringNoErr(fmt.Sprintf("Running command %v", os.Args))
	w.WriteStringNoErr(root.Options.Dump())
	if root.logFile == nil {
		w.WriteStringNoErr(`Log file: -`)
	} else {
		w.WriteStringNoErr(`Log file: ` + root.logFile.Path())
	}
}

// tearDown does clean-up after command execution.
func (root *RootCommand) tearDown(exitCode int) int {
	if err := recover(); err != nil {
		// Process panic
		exitCode = utils.ProcessPanic(err, root.Deps.Logger(), root.logFile.Path())
	}

	// Close log file
	root.logFile.TearDown(exitCode != 0)
	return exitCode
}
