package export

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	exportOp "github.com/keboola/keboola-as-code/pkg/lib/operation/llm/export"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	Force           configmap.Value[bool]   `configKey:"force" configShorthand:"f" configUsage:"skip confirmation when directory contains existing files"`
	WithSamples     configmap.Value[bool]   `configKey:"with-samples" configUsage:"include table data samples in export"`
	WithoutSamples  configmap.Value[bool]   `configKey:"without-samples" configUsage:"exclude table data samples from export"`
	SampleLimit     configmap.Value[int]    `configKey:"sample-limit" configUsage:"maximum number of rows per table sample (default: 100, max: 1000)"`
	MaxSamples      configmap.Value[int]    `configKey:"max-samples" configUsage:"maximum number of tables to sample (default: 50, max: 100)"`
}

func DefaultFlags() Flags {
	return Flags{
		SampleLimit: configmap.NewValue(exportOp.DefaultSampleLimit),
		MaxSamples:  configmap.NewValue(exportOp.DefaultMaxSamples),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: helpmsg.Read(`llm/export/short`),
		Long:  helpmsg.Read(`llm/export/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.RemoteCommandScope(cmd.Context(), f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Validate directory
			if err := validateDirectory(cmd.Context(), d, f); err != nil {
				return err
			}

			// Build options
			options := exportOp.Options{
				Force:          f.Force.Value,
				WithSamples:    f.WithSamples.Value,
				WithoutSamples: f.WithoutSamples.Value,
				SampleLimit:    f.SampleLimit.Value,
				MaxSamples:     f.MaxSamples.Value,
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(cmd.Context(), d.Clock().Now(), &cmdErr, "llm-export")

			// Export
			return exportOp.Run(cmd.Context(), options, d)
		},
	}

	// Flags
	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

	return cmd
}
