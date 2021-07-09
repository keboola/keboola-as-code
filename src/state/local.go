package state

import (
	"keboola-as-code/src/local"
	"keboola-as-code/src/model"
)

// LoadLocalState - manifest -> local files -> unified model
func (s *State) LoadLocalState() {
	for _, b := range s.manifest.Content.Branches {
		// Add branch
		if branch, err := local.LoadBranch(s.manifest.ProjectDir, b); err == nil {
			s.SetBranchLocalState(branch, b)
		} else {
			b.SetInvalid()
			s.AddLocalError(err)
		}
	}
	for _, c := range s.manifest.Content.Configs {
		// Add config
		if config, err := local.LoadConfig(s.manifest.ProjectDir, c.ConfigManifest); err == nil {
			if component, err := s.getOrLoadComponent(config.ComponentId); err == nil {
				s.SetConfigLocalState(component, config, c.ConfigManifest)
			} else {
				s.AddLocalError(err)
			}
		} else {
			c.SetInvalid()
			s.AddLocalError(err)
		}

		// Rows
		for _, r := range c.Rows {
			if row, err := local.LoadConfigRow(s.manifest.ProjectDir, r); err == nil {
				s.SetConfigRowLocalState(row, r)
			} else {
				r.SetInvalid()
				s.AddLocalError(err)
			}
		}
	}
}

func (s *State) getOrLoadComponent(componentId string) (*model.Component, error) {
	// Load component from state if present
	if component := s.GetComponent(model.ComponentKey{Id: componentId}); component != nil {
		return component, nil
	}

	// Or by API
	if component, err := s.api.GetComponent(componentId); err == nil {
		s.setComponent(component)
		return component, nil
	} else {
		return nil, err
	}
}
