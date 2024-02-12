package template

import (
	delete2 "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template/delete"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template/list"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template/rename"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template/upgrade"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/template/use"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
)

func Commands(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `template`,
		Short: helpmsg.Read(`local/template/short`),
		Long:  helpmsg.Read(`local/template/long`),
	}
	cmd.AddCommand(
		list.Command(p),
		use.Command(p),
		upgrade.Command(p),
		rename.Command(p),
		delete2.Command(p),
	)
	return cmd
}
