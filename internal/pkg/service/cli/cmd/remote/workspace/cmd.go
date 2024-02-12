package workspace

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/create"
	delete2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/delete"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/detail"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace/list"
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
		delete2.Command(p),
		detail.Command(p),
	)
	return cmd
}
