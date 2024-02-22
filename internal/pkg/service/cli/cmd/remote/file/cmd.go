package file

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/file/download"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/file/upload"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `file`,
		Short: helpmsg.Read(`remote/file/short`),
		Long:  helpmsg.Read(`remote/file/long`),
	}
	cmd.AddCommand(
		download.Command(p),
		upload.Command(p),
	)

	return cmd
}
