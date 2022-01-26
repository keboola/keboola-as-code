package links

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	// Process loaded objects
	errors := utils.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if err := m.onRemoteLoad(objectState); err != nil {
			errors.Append(err)
		}
	}

	if errors.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(utils.PrefixError(`Warning`, errors))
	}
	return nil
}
