package config

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
)

type createDeps interface {
	Components() *model.ComponentsMap
}

func AskCreateConfig(projectState *project.State, d *dialog.Dialogs, deps createDeps, f Flags) (createConfig.Options, error) {
	out := createConfig.Options{}

	// Branch
	allBranches := projectState.LocalObjects().Branches()
	branch, err := d.SelectBranch(allBranches, `Select the target branch`, f.Branch)
	if err != nil {
		return out, err
	}
	out.BranchID = branch.ID

	// Component ID
	componentID, err := askComponentID(deps, d, f.ComponentID)
	if err != nil {
		return out, err
	}
	out.ComponentID = componentID

	// Name
	name, err := d.AskObjectName(`config`, f.Name)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}

func askComponentID(deps createDeps, d *dialog.Dialogs, compID configmap.Value[string]) (keboola.ComponentID, error) {
	componentID := keboola.ComponentID("")
	components := deps.Components()

	if compID.IsSet() {
		componentID = keboola.ComponentID(strings.TrimSpace(compID.Value))
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
		if index, ok := d.SelectIndex(&prompt.SelectIndex{
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
