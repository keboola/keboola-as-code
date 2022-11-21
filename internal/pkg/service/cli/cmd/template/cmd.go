package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`template/short`),
		Long:  helpmsg.Read(`template/long`),
	}
	cmd.AddCommand(
		ListCommand(p),
		DescribeCommand(p),
		CreateCommand(p),
		repository.Commands(p),
		test.Commands(p),
	)
	return cmd
}
