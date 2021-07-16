package state

import (
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
)

// PersistDeleted objects from the filesystem
func (s *State) PersistDeleted() (deleted []manifest.Record, err error) {
	errors := utils.NewMultiError()

	// Search for deleted objects
	records := s.manifest.GetRecords()
	for _, key := range append([]string(nil), records.Keys()...) {
		recordRaw, _ := records.Get(key)
		record := recordRaw.(manifest.Record)

		if record.State().IsNotFound() {
			if err := s.localManager.DeleteModel(record); err == nil {
				deleted = append(deleted, record)
			} else {
				errors.Append(err)
			}
		}
	}

	return deleted, errors.ErrorOrNil()
}
