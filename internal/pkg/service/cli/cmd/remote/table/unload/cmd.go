package unload

import (
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/preview"
	utils2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/unload"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string]   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string]   `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	ChangedSince    configmap.Value[string]   `configKey:"changed-since" configUsage:"only export rows imported after this date"`
	ChangedUntil    configmap.Value[string]   `configKey:"changed-until" configUsage:"only export rows imported before this date"`
	Columns         configmap.Value[[]string] `configKey:"columns" configUsage:"comma-separated list of columns to export"`
	Limit           configmap.Value[uint]     `configKey:"limit" configUsage:"limit the number of exported rows"`
	Where           configmap.Value[string]   `configKey:"where" configUsage:"filter columns by value"`
	Order           configmap.Value[string]   `configKey:"order" configUsage:"order by one or more columns"`
	Format          configmap.Value[string]   `configKey:"format" configUsage:"output format (json/csv)"`
	Async           configmap.Value[bool]     `configKey:"async" configUsage:"do not wait for unload to finish"`
	Timeout         configmap.Value[string]   `configKey:"timeout" configUsage:"how long to wait for job to finish"`
}

func DefaultFlags() Flags {
	return Flags{
		Limit:   configmap.NewValue(uint(0)),
		Columns: configmap.NewValue([]string{}),
		Format:  configmap.NewValue("csv"),
		Timeout: configmap.NewValue("5m"),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `unload [table]`,
		Short: helpmsg.Read(`remote/table/unload/short`),
		Long:  helpmsg.Read(`remote/table/unload/long`),
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// flags
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken, dependencies.WithoutMasterToken())
			if err != nil {
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

			o, err := ParseUnloadOptions(tableKey, f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "remote-table-unload")

			_, err = unload.Run(cmd.Context(), o, d)
			return err
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func ParseUnloadOptions(tableKey keboola.TableKey, f Flags) (unload.Options, error) {
	o := unload.Options{TableKey: tableKey}

	o.ChangedSince = f.ChangedSince.Value
	o.ChangedUntil = f.ChangedUntil.Value
	o.Columns = f.Columns.Value
	o.Limit = f.Limit.Value
	o.Async = f.Async.Value

	e := errors.NewMultiError()

	timeout, err := time.ParseDuration(f.Timeout.Value)
	if err != nil {
		e.Append(err)
	}
	o.Timeout = timeout

	whereString := f.Where.Value
	if len(whereString) > 0 {
		for s := range strings.SplitSeq(whereString, ";") {
			whereFilter, err := preview.ParseWhereFilter(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.WhereFilters = append(o.WhereFilters, whereFilter)
		}
	}

	orderString := f.Order.Value
	if len(orderString) > 0 {
		for s := range strings.SplitSeq(orderString, ",") {
			columnOrder, err := preview.ParseColumnOrder(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.Order = append(o.Order, columnOrder)
		}
	}

	format, err := unload.ParseFormat(f.Format.Value)
	if err != nil {
		e.Append(err)
	}
	o.Format = format

	if err := e.ErrorOrNil(); err != nil {
		return unload.Options{}, err
	}

	return o, nil
}
