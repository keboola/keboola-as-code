package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/describe"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/list"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/template/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`template/short`),
		Long:  helpmsg.Read(`template/long`),
	}
	cmd.AddCommand(
		list.Command(p),
		describe.Command(p),
		create.Command(p),
		repository.Commands(p),
		test.Commands(p),
	)
	return cmd
}
