package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`template/short`),
		Long:  helpmsg.Read(`template/long`),
	}
	cmd.AddCommand(
		DescribeCommand(d),
		CreateCommand(d),
		repository.Commands(d),
	)
	return cmd
}
