package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/plan"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
			projectManifest, err := manifest.LoadManifest(root.fs)
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

			// Get plan
			encrypt := plan.Encrypt(projectState)

			// Log plan
			encrypt.Log(log.ToInfoWriter(logger))

			// Dry run?
			dryRun := root.options.GetBool("dry-run")
			if dryRun {
				logger.Info("Dry run, nothing changed.")
				return nil
			}

			// Get encryption API
			encryptionApiUrl, err := api.GetEncryptionApiUrl()
			if err != nil {
				return err
			}

			// Invoke
			encryptionApi := encryption.NewEncryptionApi(encryptionApiUrl, root.ctx, logger, false)
			if err := encrypt.Invoke(api.ProjectId(), logger, encryptionApi, projectState); err != nil {
				return err
			}

			logger.Info("Encrypt done.")
			return nil
		},
	}
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
