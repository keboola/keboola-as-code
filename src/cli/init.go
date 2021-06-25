package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"keboola-as-code/src/event"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
)

const initShortDescription = `Init local project directory and perform the first pull`
const initLongDescription = `Command "init"

Initialize local project's directory
and first time sync project from the Keboola Connection.

You will be asked to enter the Storage API host
and Storage API token from your project.
You can also enter these values
as flags or environment variables.`

func initCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: initShortDescription,
		Long:  initLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Ask for the host/token, if not specified -> to make the first step easier
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")

			// Validate options
			if err := root.ValidateOptions([]string{"ApiHost", "ApiToken"}); err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			successful := false

			// Is project directory already initialized?
			if root.options.HasProjectDirectory() {
				projectDir := root.options.ProjectDir()
				metadataDir := root.options.MetadataDir()
				root.logger.Infof(`The path "%s" is already an project directory.`, projectDir)
				root.logger.Info(`Please use a different directory or synchronize the current with "pull" command.`)
				return fmt.Errorf(`metadata directory "%s" already exists`, utils.RelPath(projectDir, metadataDir))
			}

			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Send failed event on error
			defer func() {
				if err != nil && !successful {
					event.SendCmdFailedEvent(root.start, root.logger, api, err, "init", "Init command failed.")
				}
			}()

			// Create metadata dir
			projectDir := root.options.WorkingDirectory()
			metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
			if err = os.MkdirAll(metadataDir, 0755); err != nil {
				return fmt.Errorf("cannot create metadata directory \"%s\": %s", metadataDir, err)
			}
			if err = root.options.SetProjectDirectory(projectDir); err != nil {
				return err
			}
			root.logger.Infof("Created metadata dir \"%s\".", utils.RelPath(projectDir, metadataDir))

			// Create and save manifest
			projectManifest, err := manifest.NewManifest(api.ProjectId(), api.Host(), projectDir, metadataDir)
			if err != nil {
				return err
			}
			if err = projectManifest.Save(); err != nil {
				return err
			}
			root.logger.Infof("Created manifest file \"%s\".", utils.RelPath(projectDir, projectManifest.Path()))

			// Create ENV files
			if err := createEnvFiles(root.logger, api, projectDir); err != nil {
				return err
			}

			// Send successful event
			successful = true
			event.SendCmdSuccessfulEvent(root.start, root.logger, api, "init", "Initialized local project directory.")

			// Done
			root.logger.Info("Init done. Running pull.")

			// Make first pull
			pull := root.GetCommandByName("pull")
			return pull.RunE(pull, nil)
		},
	}

	return cmd
}

func createEnvFiles(logger *zap.SugaredLogger, api *remote.StorageApi, projectDir string) error {
	// .env - with host
	envMsg := " - it contains the API host"
	envLines := []utils.FileLine{
		{Regexp: "^KBC_STORAGE_API_HOST=", Line: fmt.Sprintf(`KBC_STORAGE_API_HOST="%s"`, api.Host())},
	}
	if err := createFile(logger, projectDir, ".env", envMsg, envLines); err != nil {
		return err
	}

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
