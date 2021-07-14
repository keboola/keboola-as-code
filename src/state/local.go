package state

import (
	"keboola-as-code/src/local"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// doLoadLocalState - manifest -> local files -> unified model
func (s *State) doLoadLocalState() {
	s.localErrors = utils.NewMultiError()

	for _, b := range s.manifest.Content.Branches {
		// Add branch
		branch, found, err := local.LoadBranch(s.manifest.ProjectDir, b)
		if err == nil {
			s.SetBranchLocalState(branch, b)
		} else {
			b.SetInvalid()
			if !found {
				b.SetNotFound()
			}
			if found || !s.SkipNotFoundErr {
				s.AddLocalError(err)
			}
		}
	}

	for _, c := range s.manifest.Content.Configs {
		// Add config
		config, found, err := local.LoadConfig(s.manifest.ProjectDir, c.ConfigManifest)
		if err == nil {
			if component, err := s.getOrLoadComponent(config.ComponentId); err == nil {
				s.SetConfigLocalState(component, config, c.ConfigManifest)
			} else {
				s.AddLocalError(err)
			}
		} else {
			c.SetInvalid()
			if !found {
				c.SetNotFound()
			}
			if found || !s.SkipNotFoundErr {
				s.AddLocalError(err)
			}
		}

		// Rows
		for _, r := range c.Rows {
			row, found, err := local.LoadConfigRow(s.manifest.ProjectDir, r)
			if err == nil {
				s.SetConfigRowLocalState(row, r)
			} else {
				r.SetInvalid()
				if !found {
					r.SetNotFound()
				}
				if found || !s.SkipNotFoundErr {
					s.AddLocalError(err)
				}
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
