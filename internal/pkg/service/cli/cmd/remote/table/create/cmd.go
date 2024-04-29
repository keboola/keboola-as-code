package table

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
)

func Command(p dependencies.Provider) *cobra.Command {
	return create.Commands(p)
}
