package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapBeforeLocalSave - replace shared codes IDs by paths on local save.
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Shared code can be used only by transformation - transformation struct must be set
	transformation, ok := recipe.Object.(*model.Config)
	if !ok || transformation.Transformation == nil {
		return nil
	}

	// Link to shared code must be set
	if transformation.Transformation.LinkToSharedCode == nil {
		return nil
	}

	if err := m.replaceSharedCodeIdByPath(transformation); err != nil {
		// Log errors as warning
		m.logger.Warn(errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
	}

	return nil
}

func (m *mapper) replaceSharedCodeIdByPath(transformation *model.Config) error {
	// Get shared code
	sharedCodeKey := transformation.Transformation.LinkToSharedCode.Config
	sharedCodeState, found := m.state.GetOrNil(sharedCodeKey).(*model.ConfigState)

	// Convert LinkScript to path placeholder
	errs := errors.NewMultiError()
	transformation.Transformation.MapScripts(func(code *model.Code, script model.Script) model.Script {
		v, err := m.linkToPathPlaceholder(code, script, sharedCodeState)
		if err != nil {
			errs.Append(err)
		}
		if v != nil {
			return v
		}
		return script
	})

	// Check if shared code is found
	if !found {
		return errors.NewNestedError(
			errors.Errorf("missing shared code %s", sharedCodeKey.Desc()),
			errors.Errorf("referenced from %s", transformation.Desc()),
		)
	}

	// Check: target component of the shared code = transformation component
	if err := m.helper.CheckTargetComponent(sharedCodeState.LocalOrRemoteState().(*model.Config), transformation.ConfigKey); err != nil {
		return err
	}

	// Replace Shared Code ID -> Shared Code path
	transformation.Content.Set(model.SharedCodePathContentKey, sharedCodeState.GetRelativePath())

	return errs.ErrorOrNil()
}
