package table

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/detail"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/download"
	imp "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/import"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/preview"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/unload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/upload"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `table`,
		Short: helpmsg.Read(`remote/table/short`),
		Long:  helpmsg.Read(`remote/table/long`),
	}
	cmd.AddCommand(
		detail.Command(p),
		imp.Command(p),
		preview.Command(p),
		unload.Command(p),
		upload.Command(p),
		download.Command(p),
	)

	return cmd
}
