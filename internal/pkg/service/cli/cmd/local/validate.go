package local

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	validateConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate/config"
	validateRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate/row"
	validateSchema "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate/schema"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func ValidateCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: helpmsg.Read(`local/validate/short`),
		Long:  helpmsg.Read(`local/validate/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Options
			options := validate.Options{
				ValidateSecrets:    true,
				ValidateJSONSchema: true,
			}

			// Validate
			if err := validate.Run(cmd.Context(), projectState, options, d); err != nil {
				return err
			}

			d.Logger().Info(cmd.Context(), "Everything is good.")
			return nil
		},
	}

	cmd.AddCommand(
		ValidateConfigCommand(p),
		ValidateRowCommand(p),
		ValidateSchemaCommand(p),
	)

	return cmd
}

func ValidateConfigCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config component.id config.json",
		Short: helpmsg.Read(`local/validate/config/short`),
		Long:  helpmsg.Read(`local/validate/config/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) != 2 {
				return errors.New("please enter two arguments: component ID and JSON file path")
			}

			d, err := p.LocalCommandScope(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			o := validateConfig.Options{ComponentID: keboola.ComponentID(args[0]), ConfigPath: args[1]}
			return validateConfig.Run(cmd.Context(), o, d)
		},
	}

	return cmd
}

func ValidateRowCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "row component.id row.json",
		Short: helpmsg.Read(`local/validate/row/short`),
		Long:  helpmsg.Read(`local/validate/row/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) != 2 {
				return errors.New("please enter two arguments: component ID and JSON file path")
			}

			d, err := p.LocalCommandScope(cmd.Context(), dependencies.WithDefaultStorageAPIHost())
			if err != nil {
				return err
			}

			o := validateRow.Options{ComponentID: keboola.ComponentID(args[0]), RowPath: args[1]}
			return validateRow.Run(cmd.Context(), o, d)
		},
	}

	return cmd
}

func ValidateSchemaCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema schema.json config.json",
		Short: helpmsg.Read(`local/validate/schema/short`),
		Long:  helpmsg.Read(`local/validate/schema/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if len(args) != 2 {
				return errors.New("please enter two arguments: JSON schema path and JSON file path")
			}

			o := validateSchema.Options{SchemaPath: args[0], FilePath: args[1]}
			return validateSchema.Run(cmd.Context(), o, p.BaseScope())
		},
	}

	return cmd
}
