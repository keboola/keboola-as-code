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

You will be prompted for [values].
You can also specify them using flags or environment.

Tip:
  You can also create [object] by copying
  an existing one and running the "persist" command.
`

func createCommand(root *rootCommand) *cobra.Command {
	createConfigCmd := createConfigCommand(root)
	createRowCmd := createRowCommand(root)

	longDesc := "Command \"create\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config or config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `all needed values`)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: createShortDescription,
		Long:  longDesc,
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
	longDesc := "Command \"create config\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and component ID`)
	cmd := &cobra.Command{
		Use:   "config",
		Short: createConfigShortDescription,
		Long:  longDesc,
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
	longDesc := "Command \"create row\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and config`)
	cmd := &cobra.Command{
		Use:   "row",
		Short: createRowShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`config`, "c", ``, "config name or ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config row")
	return cmd
}
