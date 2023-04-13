package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

type createDeps interface {
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
	name, err := p.askObjectName(`branch`)
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
	branch, err := p.SelectBranch(allBranches, `Select the target branch`)
	if err != nil {
		return out, err
	}
	out.BranchID = branch.ID

	// Component ID
	componentID, err := p.askComponentID(d)
	if err != nil {
		return out, err
	}
	out.ComponentID = componentID

	// Name
	name, err := p.askObjectName(`config`)
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
	branch, err := p.SelectBranch(allBranches, `Select the target branch`)
	if err != nil {
		return out, err
	}
	out.BranchID = branch.ID

	// Config
	allConfigs := projectState.LocalObjects().ConfigsWithRowsFrom(branch.BranchKey)
	config, err := p.SelectConfig(allConfigs, `Select the target config`)
	if err != nil {
		return out, err
	}
	out.ComponentID = config.ComponentID
	out.ConfigID = config.ID

	// Name
	name, err := p.askObjectName(`config row`)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func (p *Dialogs) askObjectName(desc string) (string, error) {
	var name string
	if p.options.IsSet(`name`) {
		return p.options.GetString(`name`), nil
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

func (p *Dialogs) askComponentID(d createDeps) (keboola.ComponentID, error) {
	componentID := keboola.ComponentID("")
	components := d.Components()

	if p.options.IsSet(`component-id`) {
		componentID = keboola.ComponentID(strings.TrimSpace(p.options.GetString(`component-id`)))
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
			componentID = possibleNewComponents[index].ID
		}
	}

	if len(componentID) == 0 {
		return componentID, errors.New(`missing component ID, please specify it`)
	}

	// Check if component exists
	_, err := components.GetOrErr(componentID)
	return componentID, err
}
