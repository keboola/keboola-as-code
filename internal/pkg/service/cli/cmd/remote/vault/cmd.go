package vault

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/vault/create"
	deleteVault "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/vault/delete"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:    `vault`,
		Short:  helpmsg.Read(`remote/vault/short`),
		Long:   helpmsg.Read(`remote/vault/long`),
		Hidden: true,
	}
	cmd.AddCommand(
		create.Command(p),
		deleteVault.Command(p),
	)
	return cmd
}
