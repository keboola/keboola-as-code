package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/remote/create/branch"
)

type createDeps interface {
	Options() *options.Options
	StorageApi() (*remote.StorageApi, error)
}

func (p *Dialogs) AskWhatCreate() string {
	out, _ := p.Select(&prompt.Select{
		Label:   `What do you want to create?`,
		Options: []string{`branch`, `config`, `config row`},
	})
	return out
}

func (p *Dialogs) AskCreateBranch(d createDeps) (createBranch.Options, error) {
	out := createBranch.Options{Pull: true}

	// Name
	name, err := p.askObjectName(d, `config row`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) AskCreateConfig(d createDeps, projectState *state.State) (createConfig.Options, error) {
	out := createConfig.Options{}

	// Branch
	branch, err := p.selectTargetBranch(d, projectState)
	if err != nil {
		return out, err
	}
	out.BranchId = branch.Id

	// Component Id
	componentId, err := p.askComponentId(d)
	if err != nil {
		return out, err
	}
	out.ComponentId = componentId

	// Name
	name, err := p.askObjectName(d, `config row`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) AskCreateRow(d createDeps, projectState *state.State) (createRow.Options, error) {
	out := createRow.Options{}

	// Branch
	branch, err := p.selectTargetBranch(d, projectState)
	if err != nil {
		return out, err
	}
	out.BranchId = branch.Id

	// Config
	config, err := p.selectTargetConfig(d, projectState, branch.BranchKey)
	if err != nil {
		return out, err
	}
	out.ComponentId = config.ComponentId
	out.ConfigId = config.Id

	// Name
	name, err := p.askObjectName(d, `config row`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) askObjectName(d createDeps, desc string) (string, error) {
	var name string
	if d.Options().IsSet(`name`) {
		return d.Options().GetString(`name`), nil
	} else {
		name, _ = p.Ask(&prompt.Question{
			Label:     fmt.Sprintf(`Enter a name for the new %s`, desc),
			Validator: prompt.ValueRequired,
		})
	}
	if len(name) == 0 {
		return ``, fmt.Errorf(`missing name, please specify it`)
	}
	return name, nil
}

func (p *Dialogs) selectTargetBranch(d createDeps, projectState *state.State) (*model.BranchState, error) {
	var branch *model.BranchState
	if d.Options().IsSet(`branch`) {
		if b, err := projectState.SearchForBranch(d.Options().GetString(`branch`)); err == nil {
			branch = b
		} else {
			return nil, err
		}
	} else {
		branches := projectState.Branches()
		selectOpts := make([]string, 0)
		for _, b := range branches {
			selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectId()))
		}
		if index, ok := p.SelectIndex(&prompt.SelectIndex{
			Label:   `Select the target branch`,
			Options: selectOpts,
		}); ok {
			branch = branches[index]
		}
	}
	if branch == nil {
		return nil, fmt.Errorf(`missing branch, please specify it`)
	}

	return branch, nil
}

func (p *Dialogs) selectTargetConfig(d createDeps, projectState *state.State, branch model.BranchKey) (*model.ConfigState, error) {
	var config *model.ConfigState
	if d.Options().IsSet(`config`) {
		if c, err := projectState.SearchForConfig(d.Options().GetString(`config`), branch); err == nil {
			config = c
		} else {
			return nil, err
		}
	} else {
		// Show select prompt
		configs := projectState.ConfigsFrom(branch)
		selectOpts := make([]string, 0)
		for _, b := range configs {
			selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, b.ObjectName(), b.ObjectId()))
		}
		if index, ok := p.SelectIndex(&prompt.SelectIndex{
			Label:   `Select the target config`,
			Options: selectOpts,
		}); ok {
			config = configs[index]
		}
	}
	if config == nil {
		return nil, fmt.Errorf(`missing config, please specify it`)
	}

	return config, nil
}

func (p *Dialogs) askComponentId(d createDeps) (string, error) {
	// Get Storage API
	storageApi, err := d.StorageApi()
	if err != nil {
		return "", err
	}

	componentId := ""
	if d.Options().IsSet(`component-id`) {
		componentId = strings.TrimSpace(d.Options().GetString(`component-id`))
	} else {
		// Load components
		components, err := storageApi.NewComponentList()
		if err != nil {
			return ``, fmt.Errorf(`cannot load components list: %w`, err)
		}

		// Make select
		selectOpts := make([]string, 0)
		for _, c := range components {
			name := c.Name
			if c.Type == `extractor` || c.Type == `writer` || c.Type == `transformation` {
				name += ` ` + c.Type
			}
			item := fmt.Sprintf(`%s (%s)`, name, c.Id)
			selectOpts = append(selectOpts, item)
		}
		if index, ok := p.SelectIndex(&prompt.SelectIndex{
			Label:   `Select the target component`,
			Options: selectOpts,
		}); ok {
			componentId = components[index].Id
		}
	}

	if len(componentId) == 0 {
		return ``, fmt.Errorf(`missing component ID, please specify it`)
	}

	if _, err := storageApi.Components().Get(model.ComponentKey{Id: componentId}); err != nil {
		return ``, fmt.Errorf(`cannot load component "%s": %w`, componentId, err)
	}

	return componentId, nil
}
