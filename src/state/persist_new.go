package state

import (
	"fmt"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"path/filepath"
)

// PersistNew objects from the filesystem
func (s *State) PersistNew() (newPersisted []ObjectState, err error) {
	s.localErrors = utils.NewMultiError()
	s.newPersisted = make([]ObjectState, 0)

	s.persistNewConfigs()
	s.persistNewConfigRows()
	return s.newPersisted, s.localErrors.ErrorOrNil()
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
				s.persistNewConfig(path, relPath, matches, branch, tickets)
				break
			}
		}
	}

	// Let's wait until all new IDs are generated
	if err := tickets.Resolve(); err != nil {
		s.localErrors.Append(err)
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
				s.persistNewConfigRow(relPath, config, tickets)
				break
			}
		}
	}

	// Let's wait until all new IDs are generated
	if err := tickets.Resolve(); err != nil {
		s.localErrors.Append(err)
	}
}

func (s *State) persistNewConfig(path, relPath string, matches map[string]string, branch *BranchState, tickets *remote.TicketProvider) {
	// Get component ID
	componentId, ok := matches["component_id"]
	if !ok || componentId == "" {
		s.localErrors.Append(fmt.Errorf(
			`config's component id cannot be determined, path: "%s", path template: "%s"`,
			path,
			s.manifest.Naming.Config,
		))
		return
	}

	// Generate unique ID
	tickets.Request(func(ticket *model.Ticket) {
		key := model.ConfigKey{BranchId: branch.Id, ComponentId: componentId, Id: ticket.Id}

		// Create manifest record
		record := s.manifest.CreateOrGetRecord(key).(*model.ConfigManifest)
		record.Path = relPath
		record.ResolveParentPath(branch.BranchManifest)
		s.manifest.PersistRecord(record)

		// Load model
		if state := s.loadModel(record); state != nil {
			// Store for logs
			s.addNewPersisted(state)
		}
	})
}

func (s *State) persistNewConfigRow(relPath string, config *ConfigState, tickets *remote.TicketProvider) {
	// Generate unique ID
	tickets.Request(func(ticket *model.Ticket) {
		key := model.ConfigRowKey{BranchId: config.BranchId, ComponentId: config.ComponentId, ConfigId: config.Id, Id: ticket.Id}

		// Create manifest record
		record := s.manifest.CreateOrGetRecord(key).(*model.ConfigRowManifest)
		record.Path = relPath
		record.ResolveParentPath(config.ConfigManifest)
		s.manifest.PersistRecord(record)

		// Load model
		if state := s.loadModel(record); state != nil {
			// Store for logs
			s.addNewPersisted(state)
		}
	})
}
