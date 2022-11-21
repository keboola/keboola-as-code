package local

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

func Commands(p dependencies.Provider, envs *env.Map) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `local`,
		Short: helpmsg.Read(`local/short`),
		Long:  helpmsg.Read(`local/long`),
	}
	cmd.AddCommand(
		CreateCommand(p),
		PersistCommand(p),
		EncryptCommand(p),
		ValidateCommand(p),
		FixPathsCommand(p),
	)

	if envs.Get(`KBC_TEMPLATES_PRIVATE_BETA`) == `true` {
		cmd.AddCommand(template.Commands(p))
	}

	return cmd
}
