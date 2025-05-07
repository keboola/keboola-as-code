package preview

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	utils2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
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
	Format          configmap.Value[string]   `configKey:"format" configUsage:"output format (json/csv/pretty)"`
	Out             configmap.Value[string]   `configKey:"out" configShorthand:"o" configUsage:"export table to a file"`
	Force           configmap.Value[bool]     `configKey:"force" configUsage:"overwrite the output file if it already exists"`
}

func DefaultFlags() Flags {
	return Flags{
		Limit:   configmap.NewValue(uint(100)),
		Columns: configmap.NewValue([]string{}),
		Format:  configmap.NewValue(preview.TableFormatPretty),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `preview [table]`,
		Short: helpmsg.Read(`remote/table/preview/short`),
		Long:  helpmsg.Read(`remote/table/preview/long`),
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

			// Get table key
			tableKey := keboola.TableKey{BranchID: branch.ID}
			if len(args) < 1 {
				key, _, err := utils2.AskTable(cmd.Context(), d, branch.ID, false, configmap.NewValue(tableKey.TableID.String()))
				if err != nil {
					return err
				}
				tableKey = key
			} else if id, err := keboola.ParseTableID(args[0]); err == nil {
				tableKey.TableID = id
			} else {
				return err
			}

			// Ask options
			opts, err := parsePreviewOptions(cmd.Context(), d.Fs(), tableKey, f)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-preview")

			return preview.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}

func parsePreviewOptions(ctx context.Context, fs filesystem.Fs, tableKey keboola.TableKey, f Flags) (preview.Options, error) {
	o := preview.Options{TableKey: tableKey}

	o.ChangedSince = f.ChangedSince.Value
	o.ChangedUntil = f.ChangedUntil.Value
	o.Columns = f.Columns.Value
	o.Limit = f.Limit.Value

	e := errors.NewMultiError()

	o.Out = f.Out.Value
	if fs.Exists(ctx, o.Out) && !f.Force.IsSet() {
		e.Append(errors.Errorf(`file "%s" already exists, use the "--force" flag to overwrite it`, o.Out))
	}

	whereString := f.Where.Value
	if len(whereString) > 0 {
		for s := range strings.SplitSeq(whereString, ";") {
			whereFilter, err := ParseWhereFilter(s)
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
			columnOrder, err := ParseColumnOrder(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.Order = append(o.Order, columnOrder)
		}
	}

	format := f.Format.Value
	if !preview.IsValidFormat(format) {
		return preview.Options{}, errors.Errorf(`invalid output format "%s"`, format)
	}
	o.Format = format

	return o, nil
}

func ParseWhereFilter(s string) (preview.WhereFilter, error) {
	m := regexpcache.MustCompile(`^(\w+)(=|!=|>=|<=)([^=!<>]*)$`).FindStringSubmatch(s)
	if m == nil {
		return preview.WhereFilter{}, errors.Errorf(`invalid where filter "%s"`, s)
	}

	column := m[1]
	operator, err := keboola.ParseCompareOp(m[2])
	if err != nil {
		return preview.WhereFilter{}, err
	}
	values := strings.Split(m[3], ",")

	return preview.WhereFilter{
		Column:   column,
		Operator: operator,
		Values:   values,
	}, nil
}

func ParseColumnOrder(s string) (preview.ColumnOrder, error) {
	m := regexpcache.MustCompile(`(\w+)(?:=(asc|desc))?`).FindStringSubmatch(s)
	if m == nil {
		return preview.ColumnOrder{}, errors.Errorf(`invalid column order "%s"`, s)
	}

	column := m[1]
	orderString := m[2]
	if len(orderString) == 0 {
		orderString = "asc"
	}
	order, err := keboola.ParseColumnOrder(orderString)
	if err != nil {
		return preview.ColumnOrder{}, err
	}

	return preview.ColumnOrder{Column: column, Order: order}, nil
}
