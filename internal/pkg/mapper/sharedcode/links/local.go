package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// OnLocalChange - resolve shared codes paths, and replace them by IDs on local load.
func (m *mapper) OnLocalChange(changes *model.LocalChanges) error {
	// Process loaded objects
	errors := utils.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if err := m.onLocalLoad(objectState); err != nil {
			errors.Append(err)
		}
	}

	// Process renamed objects
	if len(changes.Renamed()) > 0 {
		if err := m.onRename(changes.Renamed()); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}
