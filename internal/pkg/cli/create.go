package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/interaction"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
- Local changes have no effect, so it is recommended to first use the "push" command.
- When the branch is created, the new state is pulled to the local directory.
`

func createCommand(root *rootCommand) *cobra.Command {
	createBranchCmd := createBranchCommand(root)
	createConfigCmd := createConfigCommand(root)
	createRowCmd := createRowCommand(root)

	longDesc := `### ` + createBranchLongDesc + "\n\n### Command \"create config/row\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config or config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `all needed values`)
	cmd := &cobra.Command{
		Use:   `create`,
		Short: createShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate project directory
			if err := ValidateMetadataFound(root.fs); err != nil {
				return err
			}

			// We ask the user what he wants to create.
			objectType, _ := root.prompt.Select(&interaction.Select{
				Label:   `What do you want to create?`,
				Options: []string{`branch`, `config`, `config row`},
			})
			switch objectType {
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

func createBranchCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: createBranchShortDescription,
		Long:  createBranchLongDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := root.logger
			o := root.options

			// This cmd can be called from parent command, so we need bind flags manually
			if err := o.BindFlagsAndEnvs(cmd.Flags()); err != nil {
				return err
			}

			// Load state
			_, api, err := loadLocalState(root)
			if err != nil {
				return err
			}

			// Name
			name, err := getName(root, `branch`)
			if err != nil {
				return err
			}

			// Create branch by API
			branch := &model.Branch{Name: name}
			if _, err := api.CreateBranch(branch); err != nil {
				return fmt.Errorf(`cannot create branch: %w`, err)
			}

			// Pull remote state
			logger.Info()
			logger.Info("The branch was successfully created.")
			logger.Info(`Pulling objects to the local directory.`)
			pull := root.GetCommandByName("pull")
			if err := pull.RunE(pull, nil); err != nil {
				return err
			}

			logger.Info(fmt.Sprintf(`Created new %s "%s".`, branch.Kind().Name, branch.Name))
			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`name`, "n", ``, "name of the new branch")
	return cmd
}

func createConfigCommand(root *rootCommand) *cobra.Command {
	longDesc := "Command \"create config\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and component ID`)
	cmd := &cobra.Command{
		Use:   "config",
		Short: createConfigShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			o := root.options

			// This cmd can be called from parent command, so we need bind flags manually
			if err := o.BindFlagsAndEnvs(cmd.Flags()); err != nil {
				return err
			}

			// Load state
			projectState, api, err := loadLocalState(root)
			if err != nil {
				return err
			}

			// Name
			name, err := getName(root, `config`)
			if err != nil {
				return err
			}

			// Branch
			branch, err := getBranch(root, projectState)
			if err != nil {
				return err
			}

			// Component ID
			componentId, err := getComponentId(root, projectState, api)
			if err != nil {
				return err
			}

			// Create object
			key := model.ConfigKey{
				BranchId:    branch.Id,
				ComponentId: componentId,
			}
			if err := createObject(root, projectState, api, key, name); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`component-id`, "c", ``, "component ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config")
	return cmd
}

func createRowCommand(root *rootCommand) *cobra.Command {
	longDesc := "Command \"create row\"\n" + createConfigOrRowLongDesc
	longDesc = strings.ReplaceAll(longDesc, `[object]`, `a new config row`)
	longDesc = strings.ReplaceAll(longDesc, `[values]`, `name, branch and config`)
	cmd := &cobra.Command{
		Use:   "row",
		Short: createRowShortDescription,
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, args []string) error {
			o := root.options

			// This cmd can be called from parent command, so we need bind flags manually
			if err := o.BindFlagsAndEnvs(cmd.Flags()); err != nil {
				return err
			}

			// Load state
			projectState, api, err := loadLocalState(root)
			if err != nil {
				return err
			}

			// Name
			name, err := getName(root, `config row`)
			if err != nil {
				return err
			}

			// Branch
			branch, err := getBranch(root, projectState)
			if err != nil {
				return err
			}

			// Config
			config, err := getConfig(root, projectState, branch.BranchKey)
			if err != nil {
				return err
			}

			// Create object
			key := model.ConfigRowKey{
				BranchId:    branch.Id,
				ComponentId: config.ComponentId,
				ConfigId:    config.Id,
			}
			if err := createObject(root, projectState, api, key, name); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().SortFlags = true
	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`config`, "c", ``, "config name or ID")
	cmd.Flags().StringP(`name`, "n", ``, "name of the new config row")
	return cmd
}

func getName(root *rootCommand, desc string) (string, error) {
	var name string
	if root.options.IsSet(`name`) {
		name = root.options.GetString(`name`)
	} else {
		name, _ = root.prompt.Ask(&interaction.Question{
			Label:     fmt.Sprintf(`Enter a name for the new %s`, desc),
			Validator: interaction.ValueRequired,
		})
	}
	if len(name) == 0 {
		return ``, fmt.Errorf(`missing name, please specify it`)
	}
	return name, nil
}

func getBranch(root *rootCommand, projectState *state.State) (*model.BranchState, error) {
	var branch *model.BranchState
	if root.options.IsSet(`branch`) {
		if b, err := projectState.SearchForBranch(root.options.GetString(`branch`)); err == nil {
			branch = b
		} else {
			return nil, err
		}
	} else {
		// Show select prompt
		branches := projectState.Branches()
		options := make([]string, 0)
		for _, b := range branches {
			options = append(options, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectId()))
		}
		if index, ok := root.prompt.SelectIndex(&interaction.Select{
			Label:   `Select the target branch`,
			Options: options,
		}); ok {
			branch = branches[index]
		}
	}
	if branch == nil {
		return nil, fmt.Errorf(`missing branch, please specify it`)
	}

	return branch, nil
}

func getConfig(root *rootCommand, projectState *state.State, branch model.BranchKey) (*model.ConfigState, error) {
	var config *model.ConfigState
	if root.options.IsSet(`config`) {
		if c, err := projectState.SearchForConfig(root.options.GetString(`config`), branch); err == nil {
			config = c
		} else {
			return nil, err
		}
	} else {
		// Show select prompt
		configs := projectState.ConfigsFrom(branch)
		options := make([]string, 0)
		for _, b := range configs {
			options = append(options, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectId()))
		}
		if index, ok := root.prompt.SelectIndex(&interaction.Select{
			Label:   `Select the target config`,
			Options: options,
		}); ok {
			config = configs[index]
		}
	}
	if config == nil {
		return nil, fmt.Errorf(`missing config, please specify it`)
	}

	return config, nil
}

func getComponentId(root *rootCommand, projectState *state.State, api *remote.StorageApi) (string, error) {
	componentId := ""
	if root.options.IsSet(`component-id`) {
		componentId = strings.TrimSpace(root.options.GetString(`component-id`))
	} else {
		// Load components
		components, err := api.NewComponentList()
		if err != nil {
			return ``, fmt.Errorf(`cannot load components list: %w`, err)
		}

		// Make select
		options := make([]string, 0)
		for _, c := range components {
			name := c.Name
			if c.Type == `extractor` || c.Type == `writer` || c.Type == `transformation` {
				name += ` ` + c.Type
			}
			item := fmt.Sprintf(`%s (%s)`, name, c.Id)
			options = append(options, item)
		}
		if index, ok := root.prompt.SelectIndex(&interaction.Select{
			Label:   `Select the target component`,
			Options: options,
		}); ok {
			componentId = components[index].Id
		}
	}

	if len(componentId) == 0 {
		return ``, fmt.Errorf(`missing component ID, please specify it`)
	}

	if _, err := projectState.Components().Get(model.ComponentKey{Id: componentId}); err != nil {
		return ``, fmt.Errorf(`cannot load component "%s": %w`, componentId, err)
	}

	return componentId, nil
}

func loadLocalState(root *rootCommand) (*state.State, *remote.StorageApi, error) {
	logger := root.logger

	// Validate project directory
	if err := ValidateMetadataFound(root.fs); err != nil {
		return nil, nil, err
	}

	// Validate token
	root.options.AskUser(root.prompt, "ApiToken")
	if err := root.ValidateOptions([]string{"ApiToken"}); err != nil {
		return nil, nil, err
	}

	// Load manifest
	projectManifest, err := manifest.LoadManifest(root.fs)
	if err != nil {
		return nil, nil, err
	}

	// Validate token and get API
	root.options.ApiHost = projectManifest.Project.ApiHost
	api, err := root.GetStorageApi()
	if err != nil {
		return nil, nil, err
	}

	// Load project local state
	stateOptions := state.NewOptions(projectManifest, api, root.ctx, logger)
	stateOptions.LoadLocalState = true
	stateOptions.LoadRemoteState = false
	projectState, ok := state.LoadState(stateOptions)
	if ok {
		logger.Debugf("Project local state has been successfully loaded.")
	} else if projectState.LocalErrors().Len() > 0 {
		return nil, nil, utils.PrefixError("project local state is invalid", projectState.LocalErrors())
	}

	return projectState, api, nil
}

func createObject(root *rootCommand, projectState *state.State, api *remote.StorageApi, key model.Key, name string) error {
	logger := root.logger

	// Generate unique ID
	ticketProvider := api.NewTicketProvider()
	ticketProvider.Request(func(ticket *model.Ticket) {
		switch k := key.(type) {
		case model.ConfigKey:
			k.Id = ticket.Id
			key = k
		case model.ConfigRowKey:
			k.Id = ticket.Id
			key = k
		default:
			panic(fmt.Errorf(`unexpecte type %T`, key))
		}
	})
	if err := ticketProvider.Resolve(); err != nil {
		return fmt.Errorf(`cannot generate new ID: %w`, err)
	}

	// Create object and save to filesystem
	uow := projectState.LocalManager().NewUnitOfWork(root.ctx)
	uow.CreateObject(key, name)
	if err := uow.Invoke(); err != nil {
		return fmt.Errorf(`cannot save object: %w`, err)
	}

	// Save manifest
	if _, err := SaveManifest(projectState.Manifest(), logger); err != nil {
		return err
	}

	logger.Info(fmt.Sprintf(`Created new %s "%s"`, key.Kind().Name, projectState.MustGet(key).Path()))
	return nil
}
