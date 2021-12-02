package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/template/repo"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:  `template`,
		Long: helpmsg.Read(`template/long`),
	}
	cmd.AddCommand(
		UseCommand(d),
		EditorCommand(d),
		DescribeCommand(d),
		repo.Commands(d),
	)
	return cmd
}
