package cli

import (
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
)

const createShortDescription = "Create config or row"
const createConfigShortDescription = "Create config"
const createRowShortDescription = "Create config row"
const createConfigOrRowLongDesc = `
Creates [object] in the local directory structure.
A new unique ID is assigned to the new object (there is no need to call "persist").
To save the new object to the project, call "push" after "create".

You will be prompted for the object specification,
or you can enter it using flags / ENVs.

Tip:
  You can also create [object] by copying
  an existing one and running the "persist" command.
`

func createCommand(root *rootCommand) *cobra.Command {
	createConfigCmd := createConfigCommand(root)
	createRowCmd := createRowCommand(root)

	cmd := &cobra.Command{
		Use:   `create`,
		Short: createShortDescription,
		Long:  "Command \"create\"\n" + strings.ReplaceAll(createConfigOrRowLongDesc, `[object]`, `a new config or config row`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// We ask the user what he wants to create.
			objectType, _ := root.prompt.Select(&interaction.Select{
				Label:   `What do you want to create?`,
				Options: []string{`config`, `config row`},
			})
			switch objectType {
			case `config`:
				return createConfigCmd.RunE(createConfigCmd, nil)
			case `config row`:
				return createRowCmd.RunE(createRowCmd, nil)
			default:
				// Non-interactive terminal -> print sub-commands.
				return cmd.Help()
			}
		},
	}

	cmd.AddCommand(createConfigCmd, createRowCmd)
	return cmd
}

func createConfigCommand(_ *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: createConfigShortDescription,
		Long:  "Command \"create config\"\n" + strings.ReplaceAll(createConfigOrRowLongDesc, `[object]`, `a new config`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`component-id`, "c", ``, "component ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config")
	return cmd
}

func createRowCommand(_ *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "row",
		Short: createRowShortDescription,
		Long:  "Command \"create row\"\n" + strings.ReplaceAll(createConfigOrRowLongDesc, `[object]`, `a new config row`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`config-id`, "c", ``, "config name or ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config row")
	return cmd
}
