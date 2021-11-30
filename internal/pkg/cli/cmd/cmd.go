package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/local"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/sync"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	versionCheck "github.com/keboola/keboola-as-code/pkg/lib/operation/remote/version/check"
)

// nolint: gochecknoinits
func init() {
	// Disable commands auto-sorting
	cobra.EnableCommandSorting = false

	// Add custom template functions
	cobra.AddTemplateFunc(`cmds`, func(root *cobra.Command) string {
		var out strings.Builder
		visitSubCommands(root, func(cmd *cobra.Command) {
			if !cmd.IsAvailableCommand() && cmd.Name() != `help` {
				return
			}

			// Separate context by new line
			level := cmdLevel(cmd) - cmdLevel(root)
			if level == 1 && !root.HasParent() {
				out.WriteString("\n")
			}

			// Indent and pad right
			template := fmt.Sprintf("%%s%%-%ds%%s", cmd.NamePadding()-level*2+6)
			out.WriteString(strings.TrimRight(fmt.Sprintf(template, strings.Repeat("  ", level), cmd.Name(), cmd.Short), " "))
			out.WriteString("\n")
		})
		return strings.Trim(out.String(), "\n")
	})
}

type Cmd = cobra.Command

type RootCommand struct {
	*Cmd
	Options *options.Options
	Logger  *zap.SugaredLogger
	Deps    *dependencies.Container
	logFile *log.File
}

// NewRootCommand creates parent of all sub-commands.
func NewRootCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer, prompt prompt.Prompt, envs *env.Map, fsFactory filesystem.Factory) *RootCommand {
	// Command definition
	root := &RootCommand{Options: options.NewOptions()}
	root.Cmd = &Cmd{
		Use:           path.Base(os.Args[0]), // name of the binary
		Version:       version.Version(),
		Short:         helpmsg.Read(`app`),
		SilenceUsage:  true,
		SilenceErrors: true, // custom error handling, see printError
		RunE: func(cmd *cobra.Command, args []string) error {
			// Print help if no command specified
			return root.Help()
		},
	}

	// Setup in/out
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)

	// Setup templates
	root.SetVersionTemplate("{{.Version}}")
	root.SetUsageTemplate(helpmsg.Read(`usage`))

	// Persistent flags for all sub-commands
	flags := root.PersistentFlags()
	flags.SortFlags = true
	flags.BoolP("help", "h", false, "print help for command")
	flags.StringP("log-file", "l", "", "path to a log file for details")
	flags.StringP("working-dir", "d", "", "use other working directory")
	flags.StringP("storage-api-token", "t", "", "storage API token from your project")
	flags.BoolP("verbose", "v", false, "print details")
	flags.BoolP("verbose-api", "", false, "log each API request and response")

	// Root command flags
	root.Flags().SortFlags = true
	root.Flags().BoolP("version", "V", false, "print version")

	// Init when flags are parsed
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
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
		root.Deps = dependencies.NewContainer(root.Context(), envs, fs, dialog.New(prompt), root.Logger, root.Options)

		// Check version
		if err := versionCheck.Run(root.Deps); err != nil {
			// Ignore error, send to logs
			root.Logger.Debugf(`Version check: %s.`, err.Error())
		}

		return nil
	}

	// Sub-commands
	root.AddCommand(
		sync.Commands(root),
		local.Commands(root),
		ci.Commands(root),
		remote.Commands(root),
	)

	// Get all sub-commands by name
	cmdsByName := make(map[string][]*cobra.Command)
	visitSubCommands(root.Cmd, func(cmd *cobra.Command) {
		if cmd.Parent() == root.Cmd {
			// Skip top-level commands
			return
		}
		cmdsByName[cmd.Name()] = append(cmdsByName[cmd.Name()], cmd)
	})

	// Create alias for all unique sub-commands.
	// For example: "kbc init" can be used instead of "kbc sync init".
	for _, cmds := range cmdsByName {
		if len(cmds) == 1 {
			alias := *cmds[0]
			alias.Hidden = true
			root.AddCommand(&alias)
		}
	}

	return root
}

func (root *RootCommand) Dependencies() *dependencies.Container {
	return root.Deps
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

	if err := root.Cmd.Execute(); err != nil {
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

	root.PrintErrln(utils.PrefixError(`Error`, modifiedErrs))
}

func (root *RootCommand) setupLogger() {
	// Get log file
	var logFileErr error
	root.logFile, logFileErr = log.NewLogFile(root.Options.LogFilePath)

	// Create logger
	root.Logger = log.NewLogger(root.OutOrStdout(), root.ErrOrStderr(), root.logFile, root.Options.Verbose)
	root.SetOut(log.ToInfoWriter(root.Logger))
	root.SetErr(log.ToWarnWriter(root.Logger))

	// Warn if user specified log file + it cannot be opened
	if logFileErr != nil && root.Options.LogFilePath != "" {
		root.Logger.Warnf("Cannot open log file: %s", logFileErr)
	}

	// Log info
	w := log.ToDebugWriter(root.Logger)
	w.WriteStringNoErr(root.Version)
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

// cmdLevel gets number of command parents.
func cmdLevel(cmd *cobra.Command) int {
	// Get number of parents
	level := 0
	cmd.VisitParents(func(_ *cobra.Command) {
		level++
	})
	return level
}

func visitSubCommands(root *cobra.Command, callback func(cmd *cobra.Command)) {
	for _, cmd := range root.Commands() {
		callback(cmd)
		visitSubCommands(cmd, callback)
	}
}
