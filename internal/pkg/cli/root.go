package cli

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/options"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
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

type rootCommand struct {
	cmd          *cobra.Command
	fsFactory    filesystem.Factory
	fs           filesystem.Fs       // filesystem abstraction
	envs         *env.Map            // ENVs from OS
	options      *options.Options    // parsed flags and env variables
	prompt       *interaction.Prompt // user interaction
	ctx          context.Context     // context for parallel operations
	api          *remote.StorageApi  // GetStorageApi should be used to initialize
	schedulerApi *scheduler.Api      // GetSchedulerApi should be used to initialize
	start        time.Time           // cmd start time
	initialized  bool                // init method was called
	logFile      afero.File          // log file instance
	logFileClear bool                // is log file temporary? if yes, it will be removed at the end, if no error occurs
	logger       *zap.SugaredLogger  // log to console and logFile
}

// NewRootCommand creates parent of all sub-commands.
func NewRootCommand(stdin io.ReadCloser, stdout io.WriteCloser, stderr io.WriteCloser, prompt *interaction.Prompt, envs *env.Map, fsFactory filesystem.Factory) *rootCommand {
	root := &rootCommand{
		fsFactory: fsFactory,
		envs:      envs,
		options:   options.NewOptions(),
		prompt:    prompt,
		ctx:       context.Background(),
		start:     time.Now(),
	}

	// Command definition
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
		if err := root.init(cmd); err != nil {
			return err
		}

		versionChecker := version.NewChecker(root.ctx, root.logger, root.envs)
		if err := versionChecker.CheckIfLatest(build.BuildVersion); err != nil {
			// Ignore error, send to logs
			root.logger.Debugf(`Version check: %s.`, err.Error())
		}

		return nil
	}

	// Sub-commands
	root.cmd.AddCommand(
		statusCommand(root),
		initCommand(root),
		validateCommand(root),
		pullCommand(root),
		pushCommand(root),
		diffCommand(root),
		persistCommand(root),
		fixPathsCommand(root),
		encryptCommand(root),
		workflowsCommand(root),
		createCommand(root),
	)

	return root
}

// Execute command or sub-command.
func (root *rootCommand) Execute() (exitCode int) {
	defer root.tearDown()
	if err := root.cmd.Execute(); err != nil {
		// Init, it can be uninitialized, if error occurred before PersistentPreRun call
		_ = root.init(root.cmd)
		// Error is already logged
		return 1
	}
	return 0
}

func (root *rootCommand) GetCommandByName(name string) *cobra.Command {
	for _, cmd := range root.cmd.Commands() {
		if cmd.Name() == name {
			return cmd
		}
	}

	return nil
}

func (root *rootCommand) ValidateOptions(required []string) error {
	if err := root.options.Validate(required); len(err) > 0 {
		root.logger.Warn("Invalid parameters:\n", err)
		return fmt.Errorf("invalid parameters, see output above")
	}
	return nil
}

// GetStorageApi returns API and initialize it first time.
func (root *rootCommand) GetStorageApi() (api *remote.StorageApi, err error) {
	if root.api == nil {
		root.api, err = root.newStorageApi()
		if err != nil {
			return nil, err
		}
	}
	return root.api, nil
}

func (root *rootCommand) GetSchedulerApi() (api *scheduler.Api, err error) {
	if root.schedulerApi == nil {
		root.schedulerApi, err = root.newSchedulerApi()
		if err != nil {
			return nil, err
		}
	}
	return root.schedulerApi, nil
}

func (root *rootCommand) newStorageApi() (*remote.StorageApi, error) {
	return remote.NewStorageApiWithToken(root.ctx, root.logger, root.options.ApiHost, root.options.ApiToken, root.options.VerboseApi)
}

func (root *rootCommand) newSchedulerApi() (*scheduler.Api, error) {
	storageApi, err := root.GetStorageApi()
	if err != nil {
		return nil, err
	}

	// Get services
	services, err := storageApi.ServicesUrlById()
	if err != nil {
		return nil, err
	}

	// Get API host
	hostName, found := services["scheduler"]
	if !found {
		return nil, fmt.Errorf(`scheduler service not found`)
	}

	return scheduler.NewSchedulerApi(root.ctx, root.logger, string(hostName), storageApi.Token().Token, false), nil
}

// tearDown makes clean-up after command execution.
func (root *rootCommand) tearDown() {
	if err := recover(); err == nil {
		if root.logFile != nil {
			if err = root.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", root.options.LogFilePath, err))
			}
		}

		// No error -> remove log file if temporary
		if root.logFileClear {
			// nolint: forbidigo
			if err = os.Remove(root.options.LogFilePath); err != nil {
				panic(fmt.Errorf("cannot remove temp log file \"%s\": %s", root.options.LogFilePath, err))
			}
		}
	} else {
		// Error -> process and close log file
		exitCode := utils.ProcessPanic(err, root.logger, root.options.LogFilePath)
		if root.logFile != nil {
			if err = root.logFile.Close(); err != nil {
				panic(fmt.Errorf("cannot close log file \"%s\": %s", root.options.LogFilePath, err))
			}
		}
		os.Exit(exitCode)
	}
}

// init sets logger and options after flags are parsed.
func (root *rootCommand) init(cmd *cobra.Command) (err error) {
	if root.initialized {
		return
	}

	// Run only once
	root.initialized = true

	// Logger must always be set up, even if there is a panic somewhere
	defer func() {
		if root.logger == nil {
			root.setupLogger()
		}
	}()

	// Temporary logger
	tmpLogger := zap.NewNop().Sugar()

	// Create filesystem abstraction
	workingDir, _ := cmd.Flags().GetString(`working-dir`)
	if root.fs, err = root.fsFactory(tmpLogger, workingDir); err != nil {
		return err
	}

	// Load values from flags and envs
	if err = root.options.Load(tmpLogger, root.envs, root.fs, cmd.Flags()); err != nil {
		return err
	}

	// Setup logger and log options load warnings
	root.setupLogger()
	root.logDebugInfo()
	root.fs.SetLogger(root.logger)

	// Return load error
	return nil
}

// setupLogger according to the options.
func (root *rootCommand) setupLogger() {
	logFile, logFileErr := root.getLogFile()
	root.logger = log.NewLogger(root.cmd.OutOrStdout(), root.cmd.ErrOrStderr(), logFile, root.options.Verbose)
	root.logFile = logFile
	root.cmd.SetOut(log.ToInfoWriter(root.logger))
	root.cmd.SetErr(log.ToWarnWriter(root.logger))

	// Warn if user specified log file and it cannot be opened
	if logFileErr != nil && !root.logFileClear {
		root.logger.Warnf("Cannot open log file: %s", logFileErr)
	}
}

func (root *rootCommand) logDebugInfo() {
	// Version
	_, err := log.ToDebugWriter(root.logger).WriteString(root.cmd.Version)
	if err != nil {
		panic(err)
	}

	// Command
	root.logger.Debugf("Running command %v", os.Args)

	// Options
	root.logger.Debug(root.options.Dump())
}

// Get log file defined in the flags or create a temp file.
// Log file can be outside project directory, so it is NOT using virtual filesystem.
func (root *rootCommand) getLogFile() (logFile *os.File, logFileErr error) {
	if len(root.options.LogFilePath) > 0 {
		root.logFileClear = false // log file defined by user will be preserved
	} else {
		// Generate a unique hash if multiple instances start simultaneously
		randomHash := ``
		randomBytes := make([]byte, 6)
		if _, err := rand.Read(randomBytes); err == nil {
			randomHash = fmt.Sprintf(`-%x`, randomBytes)
		}

		// nolint forbidigo
		root.options.LogFilePath = filepath.Join(os.TempDir(), fmt.Sprintf("keboola-as-code-%d%s.txt", time.Now().Unix(), randomHash))
		root.logFileClear = true // temp log file will be removed. It will be preserved only in case of error
	}

	// nolint: forbidigo
	logFile, logFileErr = os.OpenFile(root.options.LogFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if logFileErr != nil {
		root.options.LogFilePath = ""
	}
	return
}
