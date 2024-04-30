package table

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/table/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
)

func Command(p dependencies.Provider) *cobra.Command {
	// maintaining backward compatibility for 'remote table create' and 'remote create table'
	cmd := create.Command(p)
	cmd.Use = "table [table]"
	return cmd
}
