package repo

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:  `repo`,
		Long: helpmsg.Read(`template/repo/long`),
	}
	cmd.AddCommand(
		InitCommand(d),
		ValidateCommand(d),
		FixPathsCommand(d),
	)
	return cmd
}
