package preview

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/table/preview"
)

type Flags struct {
	StorageAPIHost string   `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	ChangedSince   string   `configKey:"changed-since" configUsage:"only export rows imported after this date"`
	ChangedUntil   string   `configKey:"changed-until" configUsage:"only export rows imported before this date"`
	Columns        []string `configKey:"columns" configUsage:"comma-separated list of columns to export"`
	Limit          uint     `configKey:"limit" configUsage:"limit the number of exported rows"`
	Where          string   `configKey:"where" configUsage:"filter columns by value"`
	Order          string   `configKey:"order" configUsage:"order by one or more columns"`
	Format         string   `configKey:"format" configUsage:"output format (json/csv/pretty)"`
	Out            string   `configKey:"out" configShorthand:"o" configUsage:"export table to a file"`
	Force          bool     `configKey:"force" configUsage:"overwrite the output file if it already exists"`
}

func DefaultFlags() *Flags {
	return &Flags{
		Limit:   100,
		Columns: []string{},
		Format:  preview.TableFormatPretty,
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `preview [table]`,
		Short: helpmsg.Read(`remote/table/preview/short`),
		Long:  helpmsg.Read(`remote/table/preview/long`),
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

			opts, err := parsePreviewOptions(cmd.Context(), d.Options(), d.Fs(), tableID)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), time.Now(), &cmdErr, "remote-table-preview")

			return preview.Run(cmd.Context(), opts, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}

func parsePreviewOptions(ctx context.Context, options *options.Options, fs filesystem.Fs, tableID keboola.TableID) (preview.Options, error) {
	o := preview.Options{TableID: tableID}

	o.ChangedSince = options.GetString("changed-since")
	o.ChangedUntil = options.GetString("changed-until")
	o.Columns = options.GetStringSlice("columns")
	o.Limit = options.GetUint("limit")

	e := errors.NewMultiError()

	o.Out = options.GetString("out")
	if fs.Exists(ctx, o.Out) && !options.GetBool("force") {
		e.Append(errors.Errorf(`file "%s" already exists, use the "--force" flag to overwrite it`, o.Out))
	}

	whereString := options.GetString("where")
	if len(whereString) > 0 {
		for _, s := range strings.Split(whereString, ";") {
			whereFilter, err := utils.ParseWhereFilter(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.WhereFilters = append(o.WhereFilters, whereFilter)
		}
	}

	orderString := options.GetString("order")
	if len(orderString) > 0 {
		for _, s := range strings.Split(orderString, ",") {
			columnOrder, err := utils.ParseColumnOrder(s)
			if err != nil {
				e.Append(err)
				continue
			}
			o.Order = append(o.Order, columnOrder)
		}
	}

	format := options.GetString("format")
	if !preview.IsValidFormat(format) {
		return preview.Options{}, errors.Errorf(`invalid output format "%s"`, format)
	}
	o.Format = format

	return o, nil
}
