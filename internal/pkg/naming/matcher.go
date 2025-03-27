package naming

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type PathMatcher struct {
	template Template
}

func NewPathMatcher(template Template) *PathMatcher {
	return &PathMatcher{template: template}
}

func (m PathMatcher) MatchConfigPath(parentKey model.Key, path model.AbsPath) (componentID keboola.ComponentID, err error) {
	parent := parentKey.Kind()
	if parent.IsBranch() {
		// Shared code
		if matched, _ := m.template.SharedCodeConfig.MatchPath(path.GetRelativePath()); matched {
			return keboola.SharedCodeComponentID, nil
		}

		// Ordinary config
		if matched, matches := m.template.Config.MatchPath(path.GetRelativePath()); matched {
			// Get component ID
			componentID, ok := matches["component_id"]
			if !ok || componentID == "" {
				return "", errors.Errorf(`config'm component id cannot be determined, path: "%s", path template: "%s"`, path.Path(), m.template.Config)
			}
			return keboola.ComponentID(componentID), nil
		}
	}

	// Config embedded in another config
	if parent.IsConfig() {
		// Variables
		if matched, _ := m.template.VariablesConfig.MatchPath(path.GetRelativePath()); matched {
			return keboola.VariablesComponentID, nil
		}
		// Scheduler
		if matched, _ := m.template.SchedulerConfig.MatchPath(path.GetRelativePath()); matched {
			return keboola.SchedulerComponentID, nil
		}
	}

	// Shared code variables, parent is config row
	if parent.IsConfigRow() && parentKey.(model.ConfigRowKey).ComponentID == keboola.SharedCodeComponentID {
		if matched, _ := m.template.VariablesConfig.MatchPath(path.GetRelativePath()); matched {
			return keboola.VariablesComponentID, nil
		}
	}

	return "", nil
}

func (m PathMatcher) MatchConfigRowPath(component *keboola.Component, path model.AbsPath) bool {
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
