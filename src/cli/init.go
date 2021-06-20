package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"time"
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

			// Send failed event - we have connection to API
			defer func() {
				if err != nil && !successful {
					sendInitFailedEvent(root, api, err)
				}
			}()

			// Create metadata dir
			projectDir := root.options.WorkingDirectory()
			metadataDir := filepath.Join(projectDir, model.MetadataDir)
			if err = os.MkdirAll(metadataDir, 0650); err != nil {
				return fmt.Errorf("cannot create metadata directory \"%s\": %s", metadataDir, err)
			}
			if err = root.options.SetProjectDirectory(projectDir); err != nil {
				return err
			}
			root.logger.Infof("Created metadata dir \"%s\".", utils.RelPath(projectDir, metadataDir))

			// Create and save manifest
			manifest, err := model.NewManifest(api.ProjectId(), api.Host(), projectDir, metadataDir)
			if err != nil {
				return err
			}
			if err = manifest.Save(); err != nil {
				return err
			}
			root.logger.Infof("Created manifest file \"%s\".", utils.RelPath(projectDir, manifest.Path))

			// Create or update ".gitignore"
			gitignorePath := filepath.Join(projectDir, ".gitignore")
			gitignoreRelPath := utils.RelPath(projectDir, gitignorePath)
			updated, err := utils.CreateOrUpdateFile(gitignorePath, []utils.FileLine{
				{Line: "/.env.local"},
			})
			if err != nil {
				return err
			}
			if updated {
				root.logger.Infof("Updated file \"%s\".", gitignoreRelPath)
			} else {
				root.logger.Infof("Created file \"%s\".", gitignoreRelPath)
			}

			// Create or update ".env.local"
			envPath := filepath.Join(projectDir, ".env.local")
			envRelPath := utils.RelPath(projectDir, envPath)
			updated, err = utils.CreateOrUpdateFile(envPath, []utils.FileLine{
				{Regexp: "^KBC_STORAGE_API_HOST=", Line: fmt.Sprintf(`KBC_STORAGE_API_HOST="%s"`, api.Host())},
				{Regexp: "^KBC_STORAGE_API_TOKEN=", Line: fmt.Sprintf(`KBC_STORAGE_API_TOKEN="%s"`, api.Token().Token)},
			})
			if err != nil {
				return err
			}
			if updated {
				root.logger.Infof("Updated file \"%s\" with the API token, keep it local and secret.", envRelPath)
			} else {
				root.logger.Infof("Created file \"%s\" with the API token, keep it local and secret.", envRelPath)
			}

			// Send successful event
			successful = true
			sendInitSuccessfulEvent(root, api)

			// Make first pull
			pull := root.GetCommandByName("pull")
			return pull.RunE(pull, nil)
		},
	}

	return cmd
}

func sendInitSuccessfulEvent(root *rootCommand, api *remote.StorageApi) {
	message := "Initialized local project directory."
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": "init",
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
	}
	event, err := api.CreateEvent("info", message, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"init\" successful event id: \"%s\"", event.Id)
	} else {
		root.logger.Warnf("Cannot send \"init\" successful event: %s", err)
	}
}

func sendInitFailedEvent(root *rootCommand, api *remote.StorageApi, err error) {
	message := "Init command failed."
	duration := time.Since(root.start)
	params := map[string]interface{}{
		"command": "init",
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := api.CreateEvent("error", message, duration, params, results)
	if err == nil {
		root.logger.Debugf("Sent \"init\" failed event id: \"%s\"", event.Id)
	} else {
		root.logger.Warnf("Cannot send \"init\" failed event: %s", err)
	}
}
