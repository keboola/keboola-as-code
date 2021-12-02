package repository

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:  `repository`,
		Long: helpmsg.Read(`template/repository/long`),
	}
	cmd.AddCommand(
		InitCommand(d),
		ValidateCommand(d),
		FixPathsCommand(d),
	)
	return cmd
}
