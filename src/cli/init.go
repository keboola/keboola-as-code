package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"keboola-as-code/src/event"
	"keboola-as-code/src/interaction"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
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
			if root.options.HasProjectDirectory() {
				projectDir := root.options.ProjectDir()
				metadataDir := root.options.MetadataDir()
				logger.Infof(`The path "%s" is already an project directory.`, projectDir)
				logger.Info(`Please use a different directory or synchronize the current with "pull" command.`)
				return fmt.Errorf(`metadata directory "%s" already exists`, utils.RelPath(projectDir, metadataDir))
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
			projectDir := root.options.WorkingDirectory()
			metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
			if err = os.MkdirAll(metadataDir, 0755); err != nil {
				return fmt.Errorf("cannot create metadata directory \"%s\": %w", metadataDir, err)
			}
			if err = root.options.SetProjectDirectory(projectDir); err != nil {
				return err
			}
			logger.Infof("Created metadata dir \"%s\".", utils.RelPath(projectDir, metadataDir))

			// Create and save manifest
			projectManifest, err := manifest.NewManifest(api.ProjectId(), api.Host(), projectDir, metadataDir)
			projectManifest.Content.AllowedBranches = allowedBranches
			if err != nil {
				return err
			}
			if err = projectManifest.Save(); err != nil {
				return err
			}
			logger.Infof("Created manifest file \"%s\".", utils.RelPath(projectDir, projectManifest.RelativePath()))

			// Create ENV files
			if err := createEnvFiles(logger, api, projectDir); err != nil {
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

func createEnvFiles(logger *zap.SugaredLogger, api *remote.StorageApi, projectDir string) error {
	// .env.local - with token value
	envLocalMsg := " - it contains the API token, keep it local and secret"
	envLocalLines := []utils.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: fmt.Sprintf(`KBC_STORAGE_API_TOKEN="%s"`, api.Token().Token)},
	}
	if err := createFile(logger, projectDir, ".env.local", envLocalMsg, envLocalLines); err != nil {
		return err
	}

	// .env.dist - with token template
	envDistMsg := ` - an ".env.local" template`
	envDistLines := []utils.FileLine{
		{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: `KBC_STORAGE_API_TOKEN=`},
	}
	if err := createFile(logger, projectDir, ".env.dist", envDistMsg, envDistLines); err != nil {
		return err
	}

	// .gitignore - to keep ".env.local" local
	gitIgnoreMsg := ` - to keep ".env.local" local`
	gitIgnoreLines := []utils.FileLine{
		{Line: "/.env.local"},
	}
	if err := createFile(logger, projectDir, ".gitignore", gitIgnoreMsg, gitIgnoreLines); err != nil {
		return err
	}

	return nil
}

func createFile(logger *zap.SugaredLogger, projectDir, path, msgSuffix string, lines []utils.FileLine) error {
	absPath := filepath.Join(projectDir, path)
	relPath := utils.RelPath(projectDir, absPath)
	updated, err := utils.CreateOrUpdateFile(absPath, lines)

	if err != nil {
		return err
	}

	if updated {
		logger.Infof("Updated file \"%s\"%s.", relPath, msgSuffix)
	} else {
		logger.Infof("Created file \"%s\"%s.", relPath, msgSuffix)
	}

	return nil
}
