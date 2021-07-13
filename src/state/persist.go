package state

import (
	"fmt"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"path/filepath"
)

// Persist created/deleted object from the filesystem
func (s *State) Persist() (newPersisted []ObjectState, err error) {
	s.localErrors = &utils.Error{}
	s.newPersisted = make([]ObjectState, 0)

	s.persistNewConfigs()
	s.persistNewConfigRows()
	if s.localErrors.Len() > 0 {
		return nil, s.localErrors
	}

	return s.newPersisted, nil
}

func (s *State) addNewPersisted(state ObjectState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.newPersisted = append(s.newPersisted, state)
}

func (s *State) persistNewConfigs() {
	tickets := s.api.NewTicketProvider()
	for _, path := range s.UntrackedDirs() {
		for _, branch := range s.Branches() {
			// Is path from the branch dir?
			relPath, err := filepath.Rel(branch.RelativePath(), path)
			if err != nil {
				continue
			}

			// Is config matching path naming/template?
			matched, matches := s.manifest.Naming.Config.MatchPath(relPath)
			if matched {
				s.persistConfig(path, relPath, matches, branch, tickets)
				break
			}
		}
	}

	// Let's wait until all new IDs are generated
	if err := tickets.Resolve(); err != nil {
		s.localErrors.Add(err)
	}
}

func (s *State) persistNewConfigRows() {
	tickets := s.api.NewTicketProvider()
	for _, path := range s.UntrackedDirs() {
		for _, config := range s.Configs() {
			// Is path from the config dir?
			relPath, err := filepath.Rel(config.RelativePath(), path)
			if err != nil {
				continue
			}

			// Is config row matching path naming/template?
			matched, _ := s.manifest.Naming.ConfigRow.MatchPath(relPath)
			if matched {
				s.persistConfigRow(path, relPath, config, tickets)
				break
			}
		}
	}

	// Let's wait until all new IDs are generated
	if err := tickets.Resolve(); err != nil {
		s.localErrors.Add(err)
	}
}

func (s *State) persistConfig(path, relPath string, matches map[string]string, branch *BranchState, tickets *remote.TicketProvider) {
	// Get component ID
	componentId, ok := matches["component_id"]
	if !ok || componentId == "" {
		s.localErrors.Add(fmt.Errorf(
			`config's component id cannot be determined, path: "%s", path template: "%s"`,
			path,
			s.manifest.Naming.Config,
		))
		return
	}

	// Load component
	component, err := s.getOrLoadComponent(componentId)
	if err != nil {
		s.localErrors.Add(utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err))
		return
	}

	// Generate unique ID
	tickets.Request(func(ticket *model.Ticket) {
		key := model.ConfigKey{BranchId: branch.Id, ComponentId: componentId, Id: ticket.Id}

		// Create manifest record
		record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigManifest)
		record.Path = relPath
		record.ResolveParentPath(branch.BranchManifest)
		s.manifest.PersistRecord(record)

		// Load model
		config, _, err := local.LoadConfig(s.manifest.ProjectDir, record)
		if err != nil {
			s.localErrors.Add(utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err))
		}

		// Update state
		state := s.SetConfigLocalState(component, config, record)

		// Store for logs
		s.addNewPersisted(state)
	})
}

func (s *State) persistConfigRow(path, relPath string, config *ConfigState, tickets *remote.TicketProvider) {
	// Generate unique ID
	tickets.Request(func(ticket *model.Ticket) {
		key := model.ConfigRowKey{BranchId: config.BranchId, ComponentId: config.ComponentId, ConfigId: config.Id, Id: ticket.Id}

		// Create manifest record
		record := s.manifest.CreateOrGetRecord(key).(*manifest.ConfigRowManifest)
		record.Path = relPath
		record.ResolveParentPath(config.ConfigManifest)
		s.manifest.PersistRecord(record)

		// Load model
		configRow, _, err := local.LoadConfigRow(s.manifest.ProjectDir, record)
		if err != nil {
			s.localErrors.Add(utils.WrapError(fmt.Sprintf(`cannot persist "%s"`, path), err))
		}

		// Update state
		state := s.SetConfigRowLocalState(configRow, record)

		// Store for logs
		s.addNewPersisted(state)
	})
}
