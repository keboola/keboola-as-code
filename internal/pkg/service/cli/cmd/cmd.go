package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	versionCheck "github.com/keboola/keboola-as-code/pkg/lib/operation/version/check"
)

// nolint: gochecknoinits
func init() {
	// Disable commands auto-sorting
	cobra.EnableCommandSorting = false

	// Add custom template functions
	cobra.AddTemplateFunc(`cmds`, func(root *cobra.Command) string {
		var out strings.Builder

		var maxCmdPathLength int
		visitSubCommands(root, func(cmd *cobra.Command) bool {
			cmdPath := strings.TrimPrefix(cmd.CommandPath(), cmd.Root().Use+` `)
			if len(cmdPath) > maxCmdPathLength {
				maxCmdPathLength = len(cmdPath)
			}
			return true
		})

		tmpl := fmt.Sprintf("  %%-%ds  %%s", maxCmdPathLength)

		visitSubCommands(root, func(cmd *cobra.Command) bool {
			if !cmd.IsAvailableCommand() && cmd.Name() != `help` {
				return false
			}

			// Separate context by new line
			level := cmdLevel(cmd) - cmdLevel(root)
			if level == 1 && !root.HasParent() {
				out.WriteString("\n")
			}

			// Indent and pad right
			cmdPath := strings.TrimPrefix(cmd.CommandPath(), cmd.Root().Use+` `)
			out.WriteString(strings.TrimRight(fmt.Sprintf(tmpl, cmdPath, cmd.Short), " "))
			out.WriteString("\n")
			return true
		})
		return strings.Trim(out.String(), "\n")
	})
}

type Cmd = cobra.Command

type RootCommand struct {
	*Cmd
	logger    log.Logger
	options   *options.Options
	fs        filesystem.Fs
	logFile   *log.File
	cmdByPath map[string]*cobra.Command
	aliases   *orderedmap.OrderedMap
}

// NewRootCommand creates parent of all sub-commands.
func NewRootCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer, envs *env.Map, fsFactory filesystem.Factory) *RootCommand {
	// Command definition
	root := &RootCommand{
		options:   options.New(),
		logger:    log.NewMemoryLogger(), // temporary logger, we don't have a path to the log file yet
		cmdByPath: make(map[string]*cobra.Command),
		aliases:   orderedmap.New(),
	}
	root.Cmd = &Cmd{
		Use:               "kbc", // name of the binary
		Version:           version.Version(),
		Short:             helpmsg.Read(`app`),
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
		SilenceUsage:      true,
		SilenceErrors:     true, // custom error handling, see printError
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
	root.SetUsageTemplate(helpmsg.Read(`usage`) + "\n")

	// Persistent flags for all sub-commands
	flags := root.PersistentFlags()
	flags.SortFlags = true
	flags.BoolP("help", "h", false, "print help for command")
	flags.StringP("log-file", "l", "", "path to a log file for details")
	flags.Bool("non-interactive", false, "disable interactive dialogs")
	flags.StringP("working-dir", "d", "", "use other working directory")
	flags.StringP("storage-api-token", "t", "", "storage API token from your project")
	flags.BoolP("verbose", "v", false, "print details")
	flags.BoolP("verbose-api", "", false, "log each API request and response")

	// Root command flags
	root.Flags().SortFlags = true
	root.Flags().BoolP("version", "V", false, "print version")

	// Init when flags are parsed
	p := &dependencies.ProviderRef{}
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Create filesystem abstraction
		var err error
		workingDir, _ := cmd.Flags().GetString(`working-dir`)
		root.fs, err = fsFactory(filesystem.WithLogger(root.logger), filesystem.WithWorkingDir(workingDir))
		if err != nil {
			return err
		}

		// Load values from flags and envs
		if err = root.options.Load(root.logger, envs, root.fs, cmd.Flags()); err != nil {
			return err
		}

		// Setup logger
		root.setupLogger()
		root.fs.SetLogger(root.logger)
		root.logger.Debug(`Working dir: `, filesystem.Join(root.fs.BasePath(), root.fs.WorkingDir()))

		// Interactive prompt
		prompt := cli.NewPrompt(os.Stdin, os.Stdout, os.Stderr, root.options.GetBool(options.NonInteractiveOpt))

		// Create dependencies provider
		p.Set(dependencies.NewProvider(cmd.Context(), envs, root.logger, root.fs, dialog.New(prompt, root.options), root.options))

		// Check version
		if err := versionCheck.Run(cmd.Context(), p.BaseDependencies()); err != nil {
			// Ignore error, send to logs
			root.logger.Debugf(`Version check: %s.`, err.Error())
		} else {
			root.logger.Debugf(`Version check: successful.`)
		}

		return nil
	}

	// Sub-commands
	root.AddCommand(
		StatusCommand(p),
		sync.Commands(p),
		ci.Commands(p),
		local.Commands(p, envs),
		remote.Commands(p, envs),
		dbt.Commands(p),
		template.Commands(p),
	)

	// Get all sub-commands by full path, for example "sync init"
	visitSubCommands(root.Cmd, func(cmd *cobra.Command) (goDeep bool) {
		cmdPath := cmd.CommandPath()
		cmdPath = strings.TrimPrefix(cmdPath, root.Use+` `)
		root.cmdByPath[cmdPath] = cmd
		return true
	})

	// Aliases
	root.addAlias(`i`, `sync init`)
	root.addAlias(`d`, `sync diff`)
	root.addAlias(`pl`, `sync pull`)
	root.addAlias(`ph`, `sync push`)
	root.addAlias(`v`, `local validate`)
	root.addAlias(`pt`, `local persist`)
	root.addAlias(`c`, `local create`)
	root.addAlias(`e`, `local encrypt`)
	root.addAlias(`init`, `sync init`)
	root.addAlias(`diff`, `sync diff`)
	root.addAlias(`pull`, `sync pull`)
	root.addAlias(`push`, `sync push`)
	root.addAlias(`validate`, `local validate`)
	root.addAlias(`persist`, `local persist`)
	root.addAlias(`create`, `local create`)
	root.addAlias(`encrypt`, `local encrypt`)
	root.addAlias(`use`, `local template use`)
	root.addAlias(`t`, `template`)
	root.addAlias(`r`, `template repository`)
	root.addAlias(`repo`, `template repository`)
	root.addAlias(`table`, `remote table`)

	// Add aliases to usage template
	root.Cmd.Annotations = map[string]string{`aliases`: root.listAliases()}

	return root
}

// Execute command or sub-command.
func (root *RootCommand) Execute() (exitCode int) {
	defer func() {
		exitCode = root.tearDown(exitCode, recover())
	}()

	if err := root.Cmd.Execute(); err != nil {
		root.printError(err)
		return 1
	}
	return 0
}

func (root *RootCommand) listAliases() string {
	// Join aliases to single line
	var lines []string
	var maxLength int
	for _, cmd := range root.aliases.Keys() {
		aliasesRaw, _ := root.aliases.Get(cmd)
		aliasesStr := strings.Join(aliasesRaw.([]string), `, `)
		lines = append(lines, aliasesStr)
		length := len(cmd)
		if length > maxLength {
			maxLength = length
		}
	}

	// Format
	var out strings.Builder
	for i, cmd := range root.aliases.Keys() {
		tmpl := fmt.Sprintf("  %%-%ds  %%s\n", maxLength)
		out.WriteString(fmt.Sprintf(tmpl, cmd, lines[i]))
	}
	return strings.TrimRight(out.String(), "\n")
}

func (root *RootCommand) addAlias(alias, cmdPath string) {
	target, found := root.cmdByPath[cmdPath]
	if !found {
		panic(errors.Errorf(`cannot create cmd alias "%s": command "%s" not found`, alias, cmdPath))
	}

	// Add alias
	use := strings.Split(target.Use, ` `)
	use[0] = alias
	aliasCmd := *target
	aliasCmd.Use = strings.Join(use, ` `)
	aliasCmd.Hidden = true
	root.AddCommand(&aliasCmd)

	// Store alias for help print
	var aliases []string
	aliasesRaw, found := root.aliases.Get(cmdPath)
	if found {
		aliases = aliasesRaw.([]string)
	}
	aliases = append(aliases, alias)
	root.aliases.Set(cmdPath, aliases)
}

func (root *RootCommand) printError(errRaw error) {
	// Convert to MultiError
	var originalErrs errors.MultiError
	if v, ok := errRaw.(errors.MultiError); ok { // nolint: errorlint
		originalErrs = v
	} else {
		originalErrs = errors.NewMultiError()
		originalErrs.Append(errRaw)
	}

	// Iterate over errors and replace message if needed
	modifiedErrs := errors.NewMultiError()
	var errDirNotFound dependencies.DirNotFoundError
	for _, err := range originalErrs.WrappedErrors() {
		switch {
		case errors.As(err, &errDirNotFound):
			root.logger.Infof(`The path "%s" is %s.`, root.fs.BasePath(), errDirNotFound.Found())
			if root.CalledAs() == `init` && errDirNotFound.Found() == dependencies.KbcProjectDir {
				root.logger.Infof(`Please use %s.`, errDirNotFound.Expected())
				root.logger.Info(`Or synchronize the current directory with the "pull" command.`)
			} else if errDirNotFound.Expected() == dependencies.KbcProjectDir {
				root.logger.Infof(`Please change working directory to %s.`, errDirNotFound.Expected())
				root.logger.Infof(`Or use the "sync init" command in %s.`, dependencies.EmptyDir)
			} else {
				root.logger.Infof(`Please use %s.`, errDirNotFound.Expected())
			}
			if errDirNotFound.Expected() == dependencies.EmptyDir {
				modifiedErrs.Append(errors.Wrapf(err, "directory is not empty"))
			} else {
				modifiedErrs.Append(errors.Wrapf(err, "neither this nor any parent directory is %s", errDirNotFound.Expected()))
			}
		case errors.Is(err, dependencies.ErrProjectManifestNotFound):
			root.logger.Infof(`Project directory must contain the "%s" file.`, projectManifest.Path())
			root.logger.Infof(`Please change working directory to a project directory.`)
			root.logger.Infof(`Or use the "sync init" command in an empty directory.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is project dir`))
		case errors.Is(err, dependencies.ErrRepositoryManifestNotFound):
			root.logger.Infof(`Repository directory must contain the "%s" file.`, repositoryManifest.Path())
			root.logger.Infof(`Please change working directory to a repository directory.`)
			root.logger.Infof(`Or use the "template repository init" command in an empty directory.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is repository dir`))
		case errors.Is(err, dependencies.ErrTemplateManifestNotFound):
			root.logger.Infof(`Template directory must contain the "%s" file.`, templateManifest.Path())
			root.logger.Infof(`You are in the template repository, but not in the template directory.`)
			root.logger.Infof(`Please change working directory to a template directory, for example "template/v1".`)
			root.logger.Infof(`Or use the "template create" command.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is template dir`))
		case errors.Is(err, dependencies.ErrMissingStorageAPIHost), errors.Is(err, dialog.ErrMissingStorageAPIHost):
			modifiedErrs.Append(errors.Wrapf(err, `missing Storage Api host, please use "--%s" flag or ENV variable "%s"`, options.StorageAPIHostOpt, root.options.GetEnvName(options.StorageAPIHostOpt)))
		case errors.Is(err, dependencies.ErrMissingStorageAPIToken), errors.Is(err, dialog.ErrMissingStorageAPIToken):
			modifiedErrs.Append(errors.Wrapf(err, `missing Storage Api token, please use "--%s" flag or ENV variable "%s"`, options.StorageAPITokenOpt, root.options.GetEnvName(options.StorageAPITokenOpt)))
		default:
			modifiedErrs.Append(err)
		}
	}

	fullErr := errors.PrefixError(modifiedErrs, "Error")
	root.logger.Debugf("Error debug log:\n%s", errors.Format(fullErr, errors.FormatWithStack(), errors.FormatWithUnwrap()))
	root.PrintErrln(errors.Format(fullErr, errors.FormatAsSentences()))
}

func (root *RootCommand) setupLogger() {
	// Get log file
	var logFileErr error
	root.logFile, logFileErr = log.NewLogFile(root.options.LogFilePath)

	// Get temporary logger
	memoryLogger, _ := root.logger.(*log.MemoryLogger)

	// Create logger
	root.logger = log.NewCliLogger(root.OutOrStdout(), root.ErrOrStderr(), root.logFile, root.options.Verbose)
	root.SetOut(root.logger.InfoWriter())
	root.SetErr(root.logger.WarnWriter())

	// Warn if user specified log file + it cannot be opened
	if logFileErr != nil && root.options.LogFilePath != "" {
		root.logger.Warnf("Cannot open log file: %s", logFileErr)
	}

	// Log info
	w := root.logger.DebugWriter()
	w.WriteString(root.Version)
	w.WriteString(fmt.Sprintf("Running command %v", os.Args))
	w.WriteString(root.options.Dump())
	if root.logFile == nil {
		w.WriteString(`Log file: -`)
	} else {
		w.WriteString(`Log file: ` + root.logFile.Path())
	}

	// Copy logs from the temporary logger
	if memoryLogger != nil {
		memoryLogger.CopyLogsTo(root.logger)
	}
}

// tearDown does clean-up after command execution.
func (root *RootCommand) tearDown(exitCode int, panicErr interface{}) int {
	// Logger may be uninitialized, if error occurred before initialization
	if _, ok := root.logger.(*log.MemoryLogger); ok {
		root.setupLogger()
	}

	if panicErr != nil {
		logFilePath := ""
		if root.logFile != nil {
			logFilePath = root.logFile.Path()
		}

		// Process panic
		exitCode = cli.ProcessPanic(panicErr, root.logger, logFilePath)
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

func visitSubCommands(root *cobra.Command, callback func(cmd *cobra.Command) (goDeep bool)) {
	for _, cmd := range root.Commands() {
		goDeep := callback(cmd)
		if goDeep {
			visitSubCommands(cmd, callback)
		}
	}
}
