package cli

import (
	"github.com/spf13/cobra"

	"keboola-as-code/src/encryption"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const encryptShortDescription = "Find unencrypted values in configurations and encrypt them"
const encryptLongDescription = `Command "encrypt"

This command searches for unencrypted values in al local configurations and encrypts them.
- The encrypted values are properties that begin with '#' and contain string.
- For example {"#someSecretProperty": "secret value"} will be transformed into {"#someSecretProperty": "KBC::ProjectSecure::<encryptedcontent>"}

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func encryptCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "encrypt",
		Short: encryptShortDescription,
		Long:  encryptLongDescription,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := root.logger

			// Validate project directory
			if err := root.ValidateOptions([]string{"projectDirectory"}); err != nil {
				return err
			}

			// Validate token
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"ApiToken"}); err != nil {
				return err
			}

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
			if err != nil {
				return err
			}

			// Validate token and get API
			root.options.ApiHost = projectManifest.Project.ApiHost
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load project local state
			stateOptions := state.NewOptions(projectManifest, api, root.ctx, logger)
			stateOptions.LoadLocalState = true
			stateOptions.LoadRemoteState = false
			projectState, ok := state.LoadState(stateOptions)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else if projectState.LocalErrors().Len() > 0 {
				return utils.PrefixError("project local state is invalid", projectState.LocalErrors())
			}

			// find and log unencrypted values
			unencryptedGroups := encryption.FindUnencrypted(projectState)
			encryption.LogGroups(unencryptedGroups, logger)

			// Dry run?
			dryRun := root.options.GetBool("dry-run")
			if dryRun {
				logger.Info("Dry run, nothing changed.")
				return nil
			}

			encryptionApiUrl, err := api.GetEncryptionApiUrl()
			if err != nil {
				return err
			}

			encryptionApi := encryption.NewEncryptionApi(encryptionApiUrl, root.ctx, logger, false)
			err = encryption.DoEncrypt(projectState, unencryptedGroups, encryptionApi)
			if err != nil {
				return err
			}
			logger.Info("Encrypt done.")
			return nil
		},
	}
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
