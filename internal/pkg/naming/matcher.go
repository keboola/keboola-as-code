package naming

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PathMatcher struct {
	template Template
}

func NewPathMatcher(template Template) *PathMatcher {
	return &PathMatcher{template: template}
}

func (m PathMatcher) MatchConfigPath(parentKey Key, path PathInProject) (componentId ComponentId, err error) {
	parent := parentKey.Kind()
	if parent.IsBranch() {
		// Shared code
		if matched, _ := m.template.SharedCodeConfig.MatchPath(path.ObjectPath); matched {
			return SharedCodeComponentId, nil
		}

		// Ordinary config
		if matched, matches := m.template.Config.MatchPath(path.ObjectPath); matched {
			// Get component ID
			componentId, ok := matches["component_id"]
			if !ok || componentId == "" {
				return "", fmt.Errorf(`config'm component id cannot be determined, path: "%s", path template: "%s"`, path.Path(), m.template.Config)
			}
			return ComponentId(componentId), nil
		}
	}

	// Config embedded in another config
	if parent.IsConfig() {
		// Variables
		if matched, _ := m.template.VariablesConfig.MatchPath(path.ObjectPath); matched {
			return VariablesComponentId, nil
		}
		// Scheduler
		if matched, _ := m.template.SchedulerConfig.MatchPath(path.ObjectPath); matched {
			return SchedulerComponentId, nil
		}
	}

	// Shared code variables, parent is config row
	if parent.IsConfigRow() && parentKey.(ConfigRowKey).ComponentId == SharedCodeComponentId {
		if matched, _ := m.template.VariablesConfig.MatchPath(path.ObjectPath); matched {
			return VariablesComponentId, nil
		}
	}

	return "", nil
}

func (m PathMatcher) MatchConfigRowPath(component *Component, path PathInProject) bool {
	// Shared code
	if component.IsSharedCode() {
		matched, _ := m.template.SharedCodeConfigRow.MatchPath(path.ObjectPath)
		return matched
	}

	// Variables
	if component.IsVariables() {
		matched, _ := m.template.VariablesValuesRow.MatchPath(path.ObjectPath)
		return matched
	}

	// Ordinary config row
	matched, _ := m.template.ConfigRow.MatchPath(path.ObjectPath)
	return matched
}
