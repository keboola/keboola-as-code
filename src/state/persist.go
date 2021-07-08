package state

import (
	"fmt"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"strings"
)

// Persist created/deleted object from the filesystem
func (s *State) Persist() (persisted []string, err error) {
	errors := &utils.Error{}
	naming := s.Manifest().Naming

	// Persist created - untracked paths
	for _, path := range s.UntrackedPaths() {
		// Object (config, config row, ...) is always represented by a dir
		if !utils.IsDir(path) {
			continue
		}

		// Search for path's parent object
		for _, object := range s.All() {
			// Is from parent directory?
			prefix := object.RelativePath() + string(os.PathSeparator)
			if !strings.HasPrefix(path, prefix) {
				continue
			}

			// Match path template?
			relPath := strings.TrimPrefix(path, prefix)
			switch v := object.(type) {
			case *BranchState:
				// Branch child object is config
				matched, matches := naming.Config.MatchPath(relPath)
				if matched {
					if err := s.persistUntrackedConfig(path, relPath, matches, v); err != nil {
						errors.Add(err)
					} else {
						persisted = append(persisted, path)
					}
					break
				}
			case *ConfigState:
				// Config child object is config row
				matched, _ := naming.ConfigRow.MatchPath(relPath)
				if matched {
					if err := s.persistUntrackedConfigRow(path, relPath, v); err != nil {
						errors.Add(err)
					} else {
						persisted = append(persisted, path)
					}
					break
				}
			}
		}
	}

	if errors.Len() > 0 {
		return nil, errors
	}

	return persisted, nil
}

func (s *State) persistUntrackedConfig(path, relPath string, matches map[string]string, branch *BranchState) error {
	// Get component ID
	var componentId string
	if v, ok := matches["component_id"]; ok && v != "" {
		componentId = v
	} else {
		return fmt.Errorf(`config component id cannot be determined, path: "%s", path template: "%s"`, path, s.manifest.Naming.Config)
	}

	// Load component
	component, err := s.getOrLoadComponent(componentId)
	if err != nil {
		return utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err)
	}

	// Create manifest record
	configId := ""
	key := model.ConfigKey{BranchId: branch.Id, ComponentId: componentId, Id: configId}
	record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigManifest)
	record.Path = relPath
	record.ResolveParentPath(branch.BranchManifest)

	// Load model
	config, err := local.LoadConfig(s.manifest.ProjectDir, record)
	if err != nil {
		return utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err)
	}

	// Update state
	s.SetConfigLocalState(component, config, record)
	return nil
}

func (s *State) persistUntrackedConfigRow(path, relPath string, config *ConfigState) error {
	// Create manifest record
	rowId := ""
	key := model.ConfigRowKey{BranchId: config.BranchId, ComponentId: config.ComponentId, ConfigId: config.Id, Id: rowId}
	record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigRowManifest)
	record.Path = relPath
	record.ResolveParentPath(config.ConfigManifest)

	// Load model
	configRow, err := local.LoadConfigRow(s.manifest.ProjectDir, record)
	if err != nil {
		return utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err)
	}

	// Update state
	s.SetConfigRowLocalState(configRow, record)
	return nil
}
