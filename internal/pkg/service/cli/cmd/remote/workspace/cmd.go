package workspace

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/create"
	deleteWorkspace "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/delete"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/detail"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/list"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `workspace`,
		Short: helpmsg.Read(`remote/workspace/short`),
		Long:  helpmsg.Read(`remote/workspace/long`),
	}
	cmd.AddCommand(
		create.Command(p),
		list.Command(p),
		deleteWorkspace.Command(p),
		detail.Command(p),
	)
	return cmd
}
