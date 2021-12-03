package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func Commands(d dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:  `local`,
		Long: helpmsg.Read(`local/long`),
	}
	cmd.AddCommand(
		ValidateCommand(d),
		PersistCommand(d),
		CreateCommand(d),
		EncryptCommand(d),
		FixPathsCommand(d),
	)
	return cmd
}
