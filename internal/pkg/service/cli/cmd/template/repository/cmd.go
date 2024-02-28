package repository

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/repository/respinit"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `repository`,
		Short: helpmsg.Read(`template/repository/short`),
		Long:  helpmsg.Read(`template/repository/long`),
	}
	cmd.AddCommand(
		respinit.Command(d),
	)
	return cmd
}
