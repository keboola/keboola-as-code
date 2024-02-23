package download

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	u "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/unload"
	utils2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/file/download"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

type Flags struct {
	StorageAPIHost configmap.Value[string]   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	ChangeSince    configmap.Value[string]   `configKey:"changed-since" configUsage:"only export rows imported after this date"`
	ChangedUntil   configmap.Value[string]   `configKey:"changed-until" configUsage:"only export rows imported before this date"`
	Columns        configmap.Value[[]string] `configKey:"columns" configUsage:"comma-separated list of columns to export"`
	Limit          configmap.Value[uint]     `configKey:"limit" configUsage:"limit the number of exported rows"`
	Where          configmap.Value[string]   `configKey:"where" configUsage:"filter columns by value"`
	Order          configmap.Value[string]   `configKey:"order" configUsage:"order by one or more columns"`
	Format         configmap.Value[string]   `configKey:"format" configUsage:"output format (json/csv)"`
	Timeout        configmap.Value[string]   `configKey:"timeout" configUsage:"how long to wait for the unload job to finish"`
	Output         configmap.Value[string]   `configKey:"output" configShorthand:"o" configUsage:"path to the destination file or directory"`
	AllowSliced    configmap.Value[bool]     `configKey:"allow-sliced" configUsage:"output sliced files as a directory containing slices as individual files"`
}

func DefaultFlags() Flags {
	return Flags{
		Limit:   configmap.NewValue(uint(0)),
		Columns: configmap.NewValue([]string{}),
		Format:  configmap.NewValue("csv"),
		Timeout: configmap.NewValue("2m"),
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

			// flags
			f := Flags{}
			if err = configmap.Bind(utils.GetBindConfig(cmd.Flags(), args), &f); err != nil {
				return err
			}

			// Get default branch
			branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(cmd.Context())
			if err != nil {
				return errors.Errorf("cannot get default branch: %w", err)
			}

			// Ask options
			tableKey := keboola.TableKey{BranchID: branch.ID}
			if len(args) == 0 {
				tableKey, _, err = utils2.AskTable(cmd.Context(), d, branch.ID, false, configmap.NewValue(tableKey.TableID.String()))
				if err != nil {
					return err
				}
			} else if id, err := keboola.ParseTableID(args[0]); err == nil {
				tableKey.TableID = id
			} else {
				return err
			}

			fileOutput, err := d.Dialogs().AskFileOutput(f.Output)
			if err != nil {
				return err
			}

			unloadOpts, err := u.ParseUnloadOptions(d.Options(), tableKey, u.Flags{
				StorageAPIHost: f.StorageAPIHost,
				ChangedSince:   f.ChangeSince,
				ChangedUntil:   f.ChangedUntil,
				Columns:        f.Columns,
				Limit:          f.Limit,
				Where:          f.Where,
				Order:          f.Order,
				Format:         f.Format,
				Timeout:        f.Timeout,
			})
			if err != nil {
				return err
			}

			unloadedFile, err := unload.Run(cmd.Context(), unloadOpts, d)
			if err != nil {
				return err
			}

			fileWithCredentials, err := d.KeboolaProjectAPI().GetFileWithCredentialsRequest(unloadedFile.FileKey).Send(cmd.Context())
			if err != nil {
				return err
			}

			downloadOpts := download.Options{
				File:        fileWithCredentials,
				Output:      fileOutput,
				AllowSliced: f.AllowSliced.Value,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-unload")

			return download.Run(cmd.Context(), downloadOpts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
