package sync

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync/diff"
	syncInit "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync/init"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync/pull"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/sync/push"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `sync`,
		Short: helpmsg.Read(`sync/short`),
		Long:  helpmsg.Read(`sync/long`),
	}
	cmd.AddCommand(
		syncInit.Command(p),
		pull.PullCommand(p),
		push.PushCommand(p),
		diff.Command(p),
	)
	return cmd
}
