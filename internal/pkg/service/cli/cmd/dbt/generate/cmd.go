package generate

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/generate/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/generate/profile"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/dbt/generate/source"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `generate`,
		Short: helpmsg.Read(`dbt/generate/short`),
		Long:  helpmsg.Read(`dbt/generate/long`),
	}
	cmd.AddCommand(
		profile.Command(p),
		source.Command(p),
		env.Command(p),
	)
	return cmd
}
