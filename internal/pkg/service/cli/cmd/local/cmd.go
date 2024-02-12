package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/encrypt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/fix_path"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/persist"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/validate"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider, envs *env.Map) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `local`,
		Short: helpmsg.Read(`local/short`),
		Long:  helpmsg.Read(`local/long`),
	}
	cmd.AddCommand(
		create.CreateCommand(p),
		persist.Command(p),
		encrypt.Command(p),
		validate.Command(p),
		fix_path.Command(p),
		template.Commands(p),
	)

	return cmd
}
