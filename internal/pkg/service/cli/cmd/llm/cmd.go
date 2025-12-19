package llm

import (
	"github.com/spf13/cobra"

	llmExport "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/llm/export"
	llmInit "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/llm/init"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `llm`,
		Short: helpmsg.Read(`llm/short`),
		Long:  helpmsg.Read(`llm/long`),
	}
	cmd.AddCommand(
		llmExport.Command(p),
		llmInit.Command(p),
	)
	return cmd
}
