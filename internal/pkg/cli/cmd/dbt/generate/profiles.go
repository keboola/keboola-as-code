package generate

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
)

func ProfilesCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `profiles`,
		Short: helpmsg.Read(`dbt/generate/profiles/short`),
		Long:  helpmsg.Read(`dbt/generate/profiles/long`),
	}
	return cmd
}
