package codes

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AfterRemoteOperation converts legacy "code_content" string -> []any.
func (m *mapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	errs := errors.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if ok, err := m.IsSharedCodeKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			if err := m.onConfigRemoteLoad(objectState.(*model.ConfigState).Remote); err != nil {
				errs.Append(err)
			}
		}
	}

	if errs.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(ctx, errors.Format(errors.PrefixError(errs, "warning"), errors.FormatAsSentences()))
	}

	return nil
}

func (m *mapper) onConfigRemoteLoad(config *model.Config) error {
	// Get "code_content" value
	targetRaw, found := config.Content.Get(model.ShareCodeTargetComponentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		config.Content.Delete(model.ShareCodeTargetComponentKey)
	}()

	// Value should be string
	target, ok := targetRaw.(string)
	if !ok {
		return errors.NewNestedError(
			errors.Errorf(`invalid %s`, config.Desc()),
			errors.Errorf(`key "%s" should be string, found "%T"`, model.ShareCodeTargetComponentKey, targetRaw),
		)
	}

	// Store target component ID to struct
	config.SharedCode = &model.SharedCodeConfig{Target: keboola.ComponentID(target)}

	errs := errors.NewMultiError()
	for _, row := range m.state.RemoteObjects().ConfigRowsFrom(config.ConfigKey) {
		if err := m.onRowRemoteLoad(config, row); err != nil {
			errs.Append(err)
		}
	}
	return errs.ErrorOrNil()
}

func (m *mapper) onRowRemoteLoad(config *model.Config, row *model.ConfigRow) error {
	// Get "code_content" value
	raw, found := row.Content.Get(model.SharedCodeContentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		row.Content.Delete(model.SharedCodeContentKey)
	}()

	// Parse value
	var scripts model.Scripts
	switch v := raw.(type) {
	case string:
		scripts = model.ScriptsFromStr(v, config.SharedCode.Target)
	case []any:
		scripts = model.ScriptsFromSlice(v)
	default:
		return errors.NewNestedError(
			errors.Errorf(`invalid %s`, row.Desc()),
			errors.Errorf(`key "%s" should be string or array, found "%T"`, model.SharedCodeContentKey, raw),
		)
	}

	// Store scripts to struct
	row.SharedCode = &model.SharedCodeRow{
		Target:  config.SharedCode.Target,
		Scripts: scripts,
	}
	return nil
}
