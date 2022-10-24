package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

type createDeps interface {
	Options() *options.Options
	Components() *model.ComponentsMap
}

func (p *Dialogs) AskWhatCreateRemote() string {
	out, _ := p.Select(&prompt.Select{
		Label:   `What do you want to create?`,
		Options: []string{`branch`},
	})
	return out
}

func (p *Dialogs) AskWhatCreateLocal() string {
	out, _ := p.Select(&prompt.Select{
		Label:   `What do you want to create?`,
		Options: []string{`config`, `config row`},
	})
	return out
}

func (p *Dialogs) AskCreateBranch(d createDeps) (createBranch.Options, error) {
	out := createBranch.Options{Pull: true}

	// Name
	name, err := p.askObjectName(d, `branch`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) AskCreateConfig(projectState *project.State, d createDeps) (createConfig.Options, error) {
	out := createConfig.Options{}

	// Branch
	allBranches := projectState.LocalObjects().Branches()
	branch, err := p.SelectBranch(d.Options(), allBranches, `Select the target branch`)
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
	name, err := p.askObjectName(d, `config`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) AskCreateRow(projectState *project.State, d createDeps) (createRow.Options, error) {
	out := createRow.Options{}

	// Branch
	allBranches := projectState.LocalObjects().Branches()
	branch, err := p.SelectBranch(d.Options(), allBranches, `Select the target branch`)
	if err != nil {
		return out, err
	}
	out.BranchId = branch.Id

	// Config
	allConfigs := projectState.LocalObjects().ConfigsWithRowsFrom(branch.BranchKey)
	config, err := p.SelectConfig(d.Options(), allConfigs, `Select the target config`)
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
		return ``, errors.New(`missing name, please specify it`)
	}
	return name, nil
}

func (p *Dialogs) askComponentId(d createDeps) (storageapi.ComponentID, error) {
	componentId := storageapi.ComponentID("")
	components := d.Components()

	if d.Options().IsSet(`component-id`) {
		componentId = storageapi.ComponentID(strings.TrimSpace(d.Options().GetString(`component-id`)))
	} else {
		// Make select
		selectOpts := make([]string, 0)
		possibleNewComponents := components.NewComponentList()
		for _, c := range possibleNewComponents {
			name := c.Name
			if c.Type == `extractor` || c.Type == `writer` || c.Type == `transformation` {
				name += ` ` + c.Type
			}
			item := fmt.Sprintf(`%s (%s)`, name, c.ID)
			selectOpts = append(selectOpts, item)
		}
		if index, ok := p.SelectIndex(&prompt.SelectIndex{
			Label:   `Select the target component`,
			Options: selectOpts,
		}); ok {
			componentId = possibleNewComponents[index].ID
		}
	}

	if len(componentId) == 0 {
		return componentId, errors.New(`missing component ID, please specify it`)
	}

	// Check if component exists
	_, err := components.GetOrErr(componentId)
	return componentId, err
}
