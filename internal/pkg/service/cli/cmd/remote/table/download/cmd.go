package download

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

type Flags struct {
	StorageAPIHost string   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	ChangeSince    string   `configKey:"changed-since" configUsage:"only export rows imported after this date"`
	ChangedUntil   string   `configKey:"changed-until" configUsage:"only export rows imported before this date"`
	Columns        []string `configKey:"columns" configUsage:"comma-separated list of columns to export"`
	Limit          uint     `configKey:"limit" configUsage:"limit the number of exported rows"`
	Where          string   `configKey:"where" configUsage:"filter columns by value"`
	Order          string   `configKey:"order" configUsage:"order by one or more columns"`
	Format         string   `configKey:"format" configUsage:"output format (json/csv)"`
	Timeout        string   `configKey:"timeout" configUsage:"how long to wait for the unload job to finish"`
	Output         string   `configKey:"output" configShorthand:"o" configUsage:"path to the destination file or directory"`
	AllowSliced    bool     `configKey:"allow-sliced" configUsage:"output sliced files as a directory containing slices as individual files"`
}

func DefaultFlags() *Flags {
	return &Flags{
		Limit:   0,
		Columns: []string{},
		Format:  "csv",
		Timeout: "2m",
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `download [table]`,
		Short: helpmsg.Read(`remote/table/download/short`),
		Long:  helpmsg.Read(`remote/table/download/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// Ask options
			var tableID keboola.TableID
			if len(args) == 0 {
				tableID, _, err = utils.AskTable(cmd.Context(), d, false)
				if err != nil {
					return err
				}
			} else {
				id, err := keboola.ParseTableID(args[0])
				if err != nil {
					return err
				}
				tableID = id
			}

			fileOutput, err := d.Dialogs().AskFileOutput()
			if err != nil {
				return err
			}

			unloadOpts, err := utils.ParseUnloadOptions(d.Options(), tableID)
			if err != nil {
				return err
			}

			unloadedFile, err := unload.Run(cmd.Context(), unloadOpts, d)
			if err != nil {
				return err
			}

			fileWithCredentials, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(unloadedFile.ID).Send(cmd.Context())
			if err != nil {
				return err
			}

			downloadOpts := download.Options{
				File:        fileWithCredentials,
				Output:      fileOutput,
				AllowSliced: d.Options().GetBool("allow-sliced"),
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-unload")

			return download.Run(cmd.Context(), downloadOpts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})
	return cmd
}
