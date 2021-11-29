package cmd

import (
	"time"

	"github.com/spf13/cobra"

	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/sync/init"
)

const (
	initShortDescription = `Init local project directory and perform the first pull`
	initLongDescription  = `Command "init"

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
)

func InitCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: initShortDescription,
		Long:  initLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := root.Deps
			start := time.Now()

			// Metadata directory is required
			if err := d.AssertMetaDirNotExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskInitOptions(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "init")
				}()
			} else {
				return err
			}

			// Init
			return initOp.Run(options, d)
		},
	}

	// Flags
	cmd.Flags().StringP("storage-api-host", "H", "", "storage API host, eg. \"connection.keboola.com\"")
	cmd.Flags().StringP("allowed-branches", "b", "main", `comma separated IDs or name globs, use "*" for all`)
	workflowsCmdFlags(cmd)

	return cmd
}
