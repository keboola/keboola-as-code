package state

import (
	"fmt"
	"keboola-as-code/src/client"
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
	persisted = append(persisted, s.persistAllConfigs(errors)...)
	persisted = append(persisted, s.persistAllConfigRows(errors)...)

	if errors.Len() > 0 {
		return nil, errors
	}

	return persisted, nil
}

func (s *State) persistAllConfigs(errors *utils.Error) (persisted []string) {
	pool := s.api.NewPool()
	for _, path := range s.UntrackedDirs() {
		// Is untracked object from a config from a branch?
		for _, branch := range s.Branches() {
			// Is from branch directory?
			prefix := branch.RelativePath() + string(os.PathSeparator)
			if !strings.HasPrefix(path, prefix) {
				continue
			}

			// Branch child object must be config
			relPath := strings.TrimPrefix(path, prefix)
			matched, matches := s.manifest.Naming.Config.MatchPath(relPath)
			if matched {
				if err := s.persistConfig(path, relPath, matches, branch, pool, errors); err == nil {
					persisted = append(persisted, path)
				} else {
					errors.Add(err)
				}
				break
			}
		}
	}

	// Let's wait until all new IDs are set
	if err := pool.StartAndWait(); err != nil {
		errors.Add(err)
	}

	return persisted
}

func (s *State) persistAllConfigRows(errors *utils.Error) (persisted []string) {
	pool := s.api.NewPool()
	for _, path := range s.UntrackedDirs() {
		// Is untracked object a row from a config?
		for _, config := range s.Configs() {
			// Is from config directory?
			prefix := config.RelativePath() + string(os.PathSeparator)
			if !strings.HasPrefix(path, prefix) {
				continue
			}

			// Branch child object must be config
			relPath := strings.TrimPrefix(path, prefix)
			matched, _ := s.manifest.Naming.ConfigRow.MatchPath(relPath)
			if matched {
				if err := s.persistConfigRow(path, relPath, config, pool, errors); err == nil {
					persisted = append(persisted, path)
				} else {
					errors.Add(err)
				}
				break
			}
		}
	}

	// Let's wait until all new IDs are set
	if err := pool.StartAndWait(); err != nil {
		errors.Add(err)
	}

	return persisted
}

func (s *State) persistConfig(path, relPath string, matches map[string]string, branch *BranchState, pool *client.Pool, errors *utils.Error) error {
	// Get component ID
	var componentId string
	if v, ok := matches["component_id"]; ok && v != "" {
		componentId = v
	} else {
		return fmt.Errorf(`config's component id cannot be determined, path: "%s", path template: "%s"`, path, s.manifest.Naming.Config)
	}

	// Load component
	component, err := s.getOrLoadComponent(componentId)
	if err != nil {
		return utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err)
	}

	// Generate unique ID
	pool.
		Request(s.api.GenerateNewIdRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			ticket := response.Result().(*model.Ticket)
			key := model.ConfigKey{BranchId: branch.Id, ComponentId: componentId, Id: ticket.Id}

			// Create manifest record
			record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigManifest)
			record.Path = relPath
			record.ResolveParentPath(branch.BranchManifest)
			s.manifest.PersistRecord(record)

			// Load model
			config, err := local.LoadConfig(s.manifest.ProjectDir, record)
			if err != nil {
				errors.Add(utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err))
			}

			// Update state
			s.SetConfigLocalState(component, config, record)

			return response
		}).
		Send()

	return nil
}

func (s *State) persistConfigRow(path, relPath string, config *ConfigState, pool *client.Pool, errors *utils.Error) error {
	// Generate unique ID
	pool.
		Request(s.api.GenerateNewIdRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			ticket := response.Result().(*model.Ticket)
			key := model.ConfigRowKey{BranchId: config.BranchId, ComponentId: config.ComponentId, ConfigId: config.Id, Id: ticket.Id}

			// Create manifest record
			record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigRowManifest)
			record.Path = relPath
			record.ResolveParentPath(config.ConfigManifest)
			s.manifest.PersistRecord(record)

			// Load model
			configRow, err := local.LoadConfigRow(s.manifest.ProjectDir, record)
			if err != nil {
				errors.Add(utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err))
			}

			// Update state
			s.SetConfigRowLocalState(configRow, record)

			return response
		}).
		Send()

	return nil
}
