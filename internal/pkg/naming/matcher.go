package naming

import (
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PathMatcher struct {
	template Template
}

func NewPathMatcher(template Template) *PathMatcher {
	return &PathMatcher{template: template}
}

func (m PathMatcher) MatchConfigPath(parentKey Key, path AbsPath) (componentId storageapi.ComponentID, err error) {
	parent := parentKey.Kind()
	if parent.IsBranch() {
		// Shared code
		if matched, _ := m.template.SharedCodeConfig.MatchPath(path.GetRelativePath()); matched {
			return storageapi.SharedCodeComponentID, nil
		}

		// Ordinary config
		if matched, matches := m.template.Config.MatchPath(path.GetRelativePath()); matched {
			// Get component ID
			componentId, ok := matches["component_id"]
			if !ok || componentId == "" {
				return "", fmt.Errorf(`config'm component id cannot be determined, path: "%s", path template: "%s"`, path.Path(), m.template.Config)
			}
			return storageapi.ComponentID(componentId), nil
		}
	}

	// Config embedded in another config
	if parent.IsConfig() {
		// Variables
		if matched, _ := m.template.VariablesConfig.MatchPath(path.GetRelativePath()); matched {
			return storageapi.VariablesComponentID, nil
		}
		// Scheduler
		if matched, _ := m.template.SchedulerConfig.MatchPath(path.GetRelativePath()); matched {
			return storageapi.SchedulerComponentID, nil
		}
	}

	// Shared code variables, parent is config row
	if parent.IsConfigRow() && parentKey.(ConfigRowKey).ComponentId == storageapi.SharedCodeComponentID {
		if matched, _ := m.template.VariablesConfig.MatchPath(path.GetRelativePath()); matched {
			return storageapi.VariablesComponentID, nil
		}
	}

	return "", nil
}

func (m PathMatcher) MatchConfigRowPath(component *storageapi.Component, path AbsPath) bool {
	// Shared code
	if component.IsSharedCode() {
		matched, _ := m.template.SharedCodeConfigRow.MatchPath(path.GetRelativePath())
		return matched
	}

	// Variables
	if component.IsVariables() {
		matched, _ := m.template.VariablesValuesRow.MatchPath(path.GetRelativePath())
		return matched
	}

	// Ordinary config row
	matched, _ := m.template.ConfigRow.MatchPath(path.GetRelativePath())
	return matched
}
