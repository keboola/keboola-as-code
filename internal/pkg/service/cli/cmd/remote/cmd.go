package remote

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/file"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/job"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/vault"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/workspace"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `remote`,
		Short: helpmsg.Read(`remote/short`),
		Long:  helpmsg.Read(`remote/long`),
	}
	cmd.AddCommand(
		create.Commands(p),
		file.Commands(p),
		job.Commands(p),
		workspace.Commands(p),
		table.Commands(p),
		vault.Commands(p),
	)

	return cmd
}
