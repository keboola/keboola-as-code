package ignore

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// SyncDirection indicates which side's value to keep for field-level ignores.
type SyncDirection int

const (
	// SyncDirectionPush keeps the remote value (copies remote → local) so the field is not pushed.
	SyncDirectionPush SyncDirection = iota
	// SyncDirectionPull keeps the local value (copies local → remote) so the field is not pulled.
	SyncDirectionPull
)

// IgnoreFields applies field-level ignore rules to the state before diffing.
// For each ignored field, the "authority" side's value is copied to the "edited" side
// so the diff sees no change for that field.
func (f *File) IgnoreFields(direction SyncDirection) error {
	for _, ignored := range f.state.IgnoredFields() {
		for _, configState := range f.state.Configs() {
			if configState.ComponentID.String() != ignored.ComponentID {
				continue
			}
			if configState.ID.String() != ignored.ConfigID {
				continue
			}
			if err := applyFieldOverride(configState, ignored.FieldName, direction); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyFieldOverride(config *model.ConfigState, fieldName string, direction SyncDirection) error {
	local := config.Local
	remote := config.Remote
	if local == nil || remote == nil {
		return nil
	}
	switch fieldName {
	case "isDisabled":
		if direction == SyncDirectionPush {
			local.IsDisabled = remote.IsDisabled
		} else {
			remote.IsDisabled = local.IsDisabled
		}
		return nil
	default:
		// Treat as dot-notation content key.
		return applyContentKeyOverride(local.Content, remote.Content, fieldName, direction)
	}
}

func applyContentKeyOverride(localContent, remoteContent *orderedmap.OrderedMap, fieldPath string, direction SyncDirection) error {
	if localContent == nil || remoteContent == nil {
		return nil
	}
	if direction == SyncDirectionPush {
		// Keep remote: copy remote value → local.
		value, found, err := remoteContent.GetNested(fieldPath)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		return localContent.SetNested(fieldPath, value)
	}
	// Keep local: copy local value → remote.
	value, found, err := localContent.GetNested(fieldPath)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return remoteContent.SetNested(fieldPath, value)
}
