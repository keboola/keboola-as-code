package detail

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	u "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/detail"
)

type Flag struct {
	StorageAPIHost string `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `detail [table]`,
		Short: helpmsg.Read(`remote/table/detail/short`),
		Long:  helpmsg.Read(`remote/table/detail/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), dependencies.WithoutMasterToken())
			if err != nil {
				return err
			}

			// flags
			f := Flag{}
			if err = configmap.Bind(u.GetBindConfig(cmd.Flags(), args), &f); err != nil {
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
				tableKey, _, err = utils.AskTable(cmd.Context(), d, branch.ID, false, configmap.NewValue(tableKey.TableID.String()))
				if err != nil {
					return err
				}
			} else if id, err := keboola.ParseTableID(args[0]); err == nil {
				tableKey.TableID = id
			} else {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-detail")

			return detail.Run(cmd.Context(), tableKey, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
