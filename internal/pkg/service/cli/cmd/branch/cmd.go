package branch

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/branch/link"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/branch/list"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/branch/status"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/branch/unlink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `branch`,
		Short: helpmsg.Read(`branch/short`),
		Long:  helpmsg.Read(`branch/long`),
	}
	cmd.AddCommand(
		link.Command(p),
		unlink.Command(p),
		status.Command(p),
		list.Command(p),
	)
	return cmd
}
