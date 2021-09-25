package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/event"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/initenv"
	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
)

const initShortDescription = `Init local project directory and perform the first pull`
const initLongDescription = `Command "init"

Initialize local project's directory
and make first sync from the Keboola Connection.

You will be prompted to define:
- storage API host
- storage API token of your project
- allowed branches
- GitHub Actions workflows

You can also enter these values
by flags or environment variables.

This CLI tool will only work with the specified "allowed branches".
Others will be ignored, although they will still exist in the project.
`

func initCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: initShortDescription,
		Long:  initLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger := root.logger
			successful := false

			// Is project directory already initialized?
			if root.fs.Exists(filesystem.MetadataDir) {
				logger.Infof(`The path "%s" is already an project directory.`, root.fs.BasePath())
				logger.Info(`Please use a different directory or synchronize the current with "pull" command.`)
				return fmt.Errorf(`metadata directory "%s" already exists`, filesystem.MetadataDir)
			}

			// Prompt user for host and token
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")

			// Validate options
			if err := root.ValidateOptions([]string{"ApiHost", "ApiToken"}); err != nil {
				return err
			}

			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Send failed event on error
			defer func() {
				if err != nil && !successful {
					event.SendCmdFailedEvent(root.start, logger, api, err, "init", "Init command failed.")
				}
			}()

			// Load all branches
			allBranches, err := api.ListBranches()
			if err != nil {
				return err
			}

			// Prompt user for allowed allBranches
			allowedBranches := root.prompt.GetAllowedBranches(
				allBranches,
				root.options.IsSet("allowed-branches"),
				root.options.GetString("allowed-branches"),
			)
			logger.Infof(`Set allowed branches: %s`, allowedBranches.String())

			// Create metadata dir
			if err = root.fs.Mkdir(filesystem.MetadataDir); err != nil {
				return fmt.Errorf("cannot create metadata directory \"%s\": %w", filesystem.MetadataDir, err)
			}
			logger.Infof("Created metadata directory \"%s\".", filesystem.MetadataDir)

			// Create and save manifest
			projectManifest, err := manifest.NewManifest(api.ProjectId(), api.Host(), root.fs)
			projectManifest.Content.AllowedBranches = allowedBranches
			if err != nil {
				return err
			}
			if err = projectManifest.Save(); err != nil {
				return err
			}
			logger.Infof("Created manifest file \"%s\".", projectManifest.Path())

			// Create ENV files
			if err := initenv.CreateEnvFiles(logger, root.fs, api); err != nil {
				return err
			}

			// Send successful event
			successful = true
			event.SendCmdSuccessfulEvent(root.start, logger, api, "init", "Initialized local project directory.")

			// Generate CI workflows
			if root.prompt.Confirm(&interaction.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
				workflows := root.GetCommandByName("workflows")
				if err := workflows.RunE(workflows, nil); err != nil {
					return err
				}
			}

			// Make first pull
			logger.Info()
			logger.Info("Init done. Running pull.")
			pull := root.GetCommandByName("pull")
			if err := pull.RunE(pull, nil); err != nil {
				return err
			}

			return nil
		},
	}

	// Flags
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("allowed-branches", "b", "main", `comma separated IDs or name globs, use "*" for all`)
	workflowsCmdFlags(cmd)

	return cmd
}
