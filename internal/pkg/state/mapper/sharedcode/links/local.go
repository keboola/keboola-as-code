package links

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *localMapper) getSharedCodeByPath(branchKey model.BranchKey, codePath string) (*model.Config, error) {
	// Get branch path - shared code path is relative to the branch path
	branch := m.state.MustGet(branchKey)
	branchPath, err := m.state.GetPath(branch)
	if err != nil {
		return nil, err
	}

	// Get key by path
	codePath = filesystem.Join(branchPath.String(), codePath)
	configStateRaw, found := m.state.GetByPath(codePath)
	if !found {
		return nil, fmt.Errorf(`missing shared code "%s"`, codePath)
	}

	// Is config?
	configState, ok := configStateRaw.(*model.Config)
	if !ok {
		return nil, fmt.Errorf(`path "%s" is not shared code config`, codePath)
	}

	// Shared code?
	if configState.ComponentId != model.SharedCodeComponentId {
		return nil, fmt.Errorf(`config "%s" is not shared code`, codePath)
	}

	// Ok
	return configState, nil
}

func (m *localMapper) linkToPathPlaceholder(code *model.Code, script model.Script, sharedCode *model.Config) (model.Script, error) {
	if link, ok := script.(model.LinkScript); ok {
		row, ok := m.state.GetOrNil(link.Target).(*model.Config)
		if !ok || sharedCode == nil {
			// Return ID placeholder, if row is not found
			return model.StaticScript{Value: m.id.format(link.Target.ConfigRowId)}, errors.PrefixError(
				fmt.Sprintf(`missing shared code %s`, link.Target.String()),
				fmt.Errorf(`referenced from %s`, code.String()),
			)
		}

		// Shared code row path
		rowPath, err := m.state.GetPath(row)
		if err != nil {
			return nil, err
		}

		// Return placeholder with relative path to the shared code
		return model.StaticScript{Value: m.path.format(rowPath.RelativePath(), code.ComponentId())}, nil
	}
	return nil, nil
}
