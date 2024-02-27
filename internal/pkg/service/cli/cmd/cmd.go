package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

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
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmdconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	versionCheck "github.com/keboola/keboola-as-code/pkg/lib/operation/version/check"
)

type RootFlag struct {
	Version bool `configKey:"version" configShorthand:"V" configUsage:"print version"`
}

type GlobalFlags struct {
	Help            bool   `configKey:"help" configShorthand:"h" configUsage:"print help for command"`
	LogFile         string `configKey:"log-file" configShorthand:"l" configUsage:"path to a log file for details"`
	LogFormat       string `configKey:"log-format" configUsage:"format of stdout and stderr"`
	NonInteractive  bool   `configKey:"non-interactive" configUsage:"disable interactive dialogs"`
	WorkingDir      string `configKey:"working-dir" configShorthand:"d" configUsage:"use other working directory"`
	StorageAPIToken string `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Verbose         bool   `configKey:"verbose" configShorthand:"v" configUsage:"print details"`
	VerboseAPI      bool   `configKey:"verbose-api" configUsage:"log each API request and response"`
	VersionCheck    bool   `configKey:"version-check" configUsage:"checks if there is a newer version of the CLI"`
}

func DefaultGlobalFlags() GlobalFlags {
	return GlobalFlags{
		VersionCheck: true,
		LogFormat:    "console",
	}
}

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
	globalFlags GlobalFlags
	logger      log.Logger
	fs          filesystem.Fs
	logFile     *log.File
	logFormat   log.LogFormat
	cmdByPath   map[string]*cobra.Command
	aliases     *orderedmap.OrderedMap
}

// NewRootCommand creates parent of all sub-commands.
func NewRootCommand(stdin io.Reader, stdout io.Writer, stderr io.Writer, osEnvs *env.Map, fsFactory filesystem.Factory) *RootCommand {
	// Command definition
	root := &RootCommand{
		logger:      log.NewMemoryLogger(), // temporary logger, we don't have a path to the log file yet
		cmdByPath:   make(map[string]*cobra.Command),
		aliases:     orderedmap.New(),
		globalFlags: DefaultGlobalFlags(),
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
	fmt.Println("dsadasdas ", root.Cmd)

	// Setup in/out
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)

	// Setup templates
	root.SetVersionTemplate("{{.Version}}")
	root.SetUsageTemplate(helpmsg.Read(`usage`) + "\n")

	// Persistent flags for all sub-commands
	configmap.MustGenerateFlags(root.PersistentFlags(), DefaultGlobalFlags())

	// Root command flags
	configmap.MustGenerateFlags(root.Flags(), RootFlag{})

	// Init when flags are parsed
	p := &dependencies.ProviderRef{}
	root.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		// Create filesystem abstraction
		var err error
		workingDir, _ := cmd.Flags().GetString(`working-dir`)
		root.fs, err = fsFactory(cmd.Context(), filesystem.WithLogger(root.logger), filesystem.WithWorkingDir(workingDir))
		if err != nil {
			return err
		}

		// Load values from flags and envs
		if err = Load(cmd.Context(), root.logger, osEnvs, root.fs, cmd.Flags()); err != nil {
			return err
		}

		// Setup logger
		root.setupLogger()
		root.fs.SetLogger(root.logger)
		root.logger.Debugf(cmd.Context(), `Working dir: %s`, filesystem.Join(root.fs.BasePath(), root.fs.WorkingDir()))

		// Interactive prompt
		prompt := cli.NewPrompt(os.Stdin, stdout, stderr, root.globalFlags.NonInteractive)

		// Create process abstraction
		proc := servicectx.New()

		// Load ENVs
		envs := loadEnvFiles(cmd.Context(), root.logger, osEnvs, root.fs)

		// Create dependencies provider
		p.Set(dependencies.NewProvider(
			cmd.Context(),
			root.logger,
			proc,
			root.fs,
			dialog.New(prompt),
			envs,
			stdout,
			stderr,
		))

		// Check version
		if err := versionCheck.Run(cmd.Context(), root.globalFlags.VersionCheck, p.BaseScope()); err != nil {
			// Ignore error, send to logs
			root.logger.Debugf(cmd.Context(), `Version check: %s.`, err.Error())
		} else {
			root.logger.Debug(cmd.Context(), `Version check: successful.`)
		}

		return nil
	}

	// Sub-commands
	root.AddCommand(
		StatusCommand(p),
		sync.Commands(p),
		ci.Commands(p),
		local.Commands(p, osEnvs),
		remote.Commands(p, osEnvs),
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
			root.logger.Infof(root.Context(), `The path "%s" is %s.`, root.fs.BasePath(), errDirNotFound.Found())
			switch {
			case root.CalledAs() == `init` && errDirNotFound.Found() == dependencies.KbcProjectDir:
				root.logger.Infof(root.Context(), `Please use %s.`, errDirNotFound.Expected())
				root.logger.Info(root.Context(), `Or synchronize the current directory with the "pull" command.`)
			case errDirNotFound.Expected() == dependencies.KbcProjectDir:
				root.logger.Infof(root.Context(), `Please change working directory to %s.`, errDirNotFound.Expected())
				root.logger.Infof(root.Context(), `Or use the "sync init" command in %s.`, dependencies.EmptyDir)
			default:
				root.logger.Infof(root.Context(), `Please use %s.`, errDirNotFound.Expected())
			}
			if errDirNotFound.Expected() == dependencies.EmptyDir {
				modifiedErrs.Append(errors.Wrapf(err, "directory is not empty"))
			} else {
				modifiedErrs.Append(errors.Wrapf(err, "neither this nor any parent directory is %s", errDirNotFound.Expected()))
			}
		case errors.Is(err, dependencies.ErrProjectManifestNotFound):
			root.logger.Infof(root.Context(), `Project directory must contain the "%s" file.`, projectManifest.Path())
			root.logger.Infof(root.Context(), `Please change working directory to a project directory.`)
			root.logger.Infof(root.Context(), `Or use the "sync init" command in an empty directory.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is project dir`))
		case errors.Is(err, dependencies.ErrRepositoryManifestNotFound):
			root.logger.Infof(root.Context(), `Repository directory must contain the "%s" file.`, repositoryManifest.Path())
			root.logger.Infof(root.Context(), `Please change working directory to a repository directory.`)
			root.logger.Infof(root.Context(), `Or use the "template repository init" command in an empty directory.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is repository dir`))
		case errors.Is(err, dependencies.ErrTemplateManifestNotFound):
			root.logger.Infof(root.Context(), `Template directory must contain the "%s" file.`, templateManifest.Path())
			root.logger.Infof(root.Context(), `You are in the template repository, but not in the template directory.`)
			root.logger.Infof(root.Context(), `Please change working directory to a template directory, for example "template/v1".`)
			root.logger.Infof(root.Context(), `Or use the "template create" command.`)
			modifiedErrs.Append(errors.Wrapf(err, `none of this and parent directories is template dir`))
		case errors.Is(err, dependencies.ErrMissingStorageAPIHost), errors.Is(err, dialog.ErrMissingStorageAPIHost):
			modifiedErrs.Append(errors.Wrapf(err, `missing Storage Api host, please use "--%s" flag or ENV variable "%s"`, cmdconfig.StorageAPIHostOpt, env.NewNamingConvention(cmdconfig.ENVPrefix).FlagToEnv(cmdconfig.StorageAPIHostOpt)))
		case errors.Is(err, dependencies.ErrMissingStorageAPIToken), errors.Is(err, dialog.ErrMissingStorageAPIToken):
			modifiedErrs.Append(errors.Wrapf(err, `missing Storage Api token, please use "--%s" flag or ENV variable "%s"`, cmdconfig.StorageAPITokenOpt, env.NewNamingConvention(cmdconfig.ENVPrefix).FlagToEnv(cmdconfig.StorageAPITokenOpt)))
		default:
			modifiedErrs.Append(err)
		}
	}

	fullErr := errors.PrefixError(modifiedErrs, "Error")
	root.logger.Debugf(root.Context(), "Error debug log:\n%s", errors.Format(fullErr, errors.FormatWithStack(), errors.FormatWithUnwrap()))
	root.PrintErrln(errors.Format(fullErr, errors.FormatAsSentences()))
}

func (root *RootCommand) setupLogger() {
	// Get log file
	var logFileErr error
	root.logFile, logFileErr = log.NewLogFile(root.globalFlags.LogFile)

	var logFormatErr error
	root.logFormat, logFormatErr = log.NewLogFormat(root.globalFlags.LogFormat)

	// Get temporary logger
	memoryLogger, _ := root.logger.(*log.MemoryLogger)

	// Create logger
	root.logger = log.NewCliLogger(root.OutOrStdout(), root.ErrOrStderr(), root.logFile, root.logFormat, root.globalFlags.Verbose)

	// Warn if user specified log file + it cannot be opened
	if logFileErr != nil && root.globalFlags.LogFile != "" {
		root.logger.Warnf(root.Context(), "Cannot open log file: %s", logFileErr)
	}

	// Warn if user specified invalid log format
	if logFormatErr != nil {
		root.logger.Warnf(root.Context(), "Invalid log format: %s", logFormatErr)
	}

	// Log info
	root.logger.Debug(root.Context(), root.Version)
	root.logger.Debugf(root.Context(), "Running command %v", os.Args)
	// root.logger.Debug(root.Context(), root.options.Dump())
	if root.logFile == nil {
		root.logger.Debug(root.Context(), `Log file: -`)
	} else {
		root.logger.Debug(root.Context(), `Log file: `+root.logFile.Path())
	}

	// Copy logs from the temporary logger
	if memoryLogger != nil {
		memoryLogger.CopyLogsTo(root.logger)
	}
}

// tearDown does clean-up after command execution.
func (root *RootCommand) tearDown(exitCode int, panicErr any) int {
	// Logger may be uninitialized, if error occurred before initialization
	if _, ok := root.logger.(*log.MemoryLogger); ok {
		root.setupLogger()
	}

	if panicErr != nil {
		logFilePath := ""
		if root.logFile != nil {
			logFilePath = root.logFile.Path()
		}

		ctx := root.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		// Process panic
		exitCode = cli.ProcessPanic(ctx, panicErr, root.logger, logFilePath)
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

func Load(ctx context.Context, _ log.Logger, osEnvs *env.Map, fs filesystem.Fs, flags *pflag.FlagSet) error {
	// Load ENVs from OS and files
	envs := loadEnvFiles(ctx, log.NewNopLogger(), osEnvs, fs)

	// Define mapping between flag and field path
	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		return orderedmap.PathFromStr(flag.Name), true
	}

	// Bind all flags and corresponding ENVs
	if _, err := configmap.BindToViper(viper.New(), flagToField, configmap.BindConfig{Flags: flags, Envs: envs, EnvNaming: env.NewNamingConvention(cmdconfig.ENVPrefix)}); err != nil {
		return err
	}

	return nil
}
