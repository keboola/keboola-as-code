package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/data"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/fixpath"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/validate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `local`,
		Short: helpmsg.Read(`local/short`),
		Long:  helpmsg.Read(`local/long`),
	}
	cmd.AddCommand(
		create.Command(p),
		data.Command(p),
		persist.Command(p),
		encrypt.Command(p),
		validate.Command(p),
		fixpath.Command(p),
		template.Commands(p),
	)

	return cmd
}
