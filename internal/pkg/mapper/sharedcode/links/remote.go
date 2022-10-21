package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *mapper) AfterRemoteOperation(_ context.Context, changes *model.RemoteChanges) error {
	// Process loaded objects
	errs := errors.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if err := m.onRemoteLoad(objectState); err != nil {
			errs.Append(err)
		}
	}

	if errs.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
	}
	return nil
}
