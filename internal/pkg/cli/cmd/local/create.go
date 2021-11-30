package local

import (
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/remote/create/branch"
)

const (
	createShortDescription       = "Create branch, config or row"
	createBranchShortDescription = "Create branch"
	createConfigShortDescription = "Create config"
	createRowShortDescription    = "Create config row"
	createConfigOrRowLongDesc    = `
Creates [object] in the local directory structure.
A new unique ID is assigned to the new object (there is no need to call "persist").
To save the new object to the project, call "push" after the "create".

You will be prompted for [values].
You can also specify them using flags or environment.

Tip:
  You can also create [object] by copying
  an existing one and running the "persist" command.
`
)

const createBranchLongDesc = `Command "create branch"

- Creates a new dev branch in the project remote state.
- The new branch will be a copy of the current remote state of the main branch.
- It is recommended to first "push" local changes in the main branch if any.
- When the branch is created, the new state is pulled to the local directory.
`

func CreateCommand(depsProvider dependencies.Provider) *cobra.Command {
	createBranchCmd := CreateBranchCommand(depsProvider)
	createConfigCmd := CreateConfigCommand(depsProvider)
	createRowCmd := CreateRowCommand(depsProvider)

	longDesc := `### ` + createBranchLongDesc + "\n\n### Command \"create config/row\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config or config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `all needed values`)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: createShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// We ask the user what he wants to create.
			switch d.Dialogs().AskWhatCreate() {
			case `branch`:
				return createBranchCmd.RunE(createBranchCmd, nil)
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

	cmd.AddCommand(createBranchCmd, createConfigCmd, createRowCmd)
	return cmd
}

func CreateBranchCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: createBranchShortDescription,
		Long:  createBranchLongDesc,
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateBranch(d)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "create-branch")
				}()
			} else {
				return err
			}

			// Create branch
			return createBranch.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`name`, "n", ``, "name of the new branch")
	return cmd
}

// nolint: dupl
func CreateConfigCommand(depsProvider dependencies.Provider) *cobra.Command {
	longDesc := "Command \"create config\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and component ID`)
	cmd := &cobra.Command{
		Use:   "config",
		Short: createConfigShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateConfig(d, createConfig.LoadStateOptions())
			if err != nil {
				return err
			}

			// Create config
			return createConfig.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`component-id`, "c", ``, "component ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config")
	return cmd
}

// nolint: dupl
func CreateRowCommand(depsProvider dependencies.Provider) *cobra.Command {
	longDesc := "Command \"create row\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and config`)
	cmd := &cobra.Command{
		Use:   "row",
		Short: createRowShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			d := depsProvider.Dependencies()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options, err := d.Dialogs().AskCreateRow(d, createRow.LoadStateOptions())
			if err != nil {
				return err
			}

			// Create row
			return createRow.Run(options, d)
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`config`, "c", ``, "config name or ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config row")
	return cmd
}
