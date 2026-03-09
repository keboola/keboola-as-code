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
			applyFieldOverride(configState, ignored.FieldName, direction)
		}
	}
	return nil
}

func applyFieldOverride(config *model.ConfigState, fieldName string, direction SyncDirection) {
	local := config.Local
	remote := config.Remote
	if local == nil || remote == nil {
		return
	}
	switch fieldName {
	case "isDisabled":
		if direction == SyncDirectionPush {
			local.IsDisabled = remote.IsDisabled
		} else {
			remote.IsDisabled = local.IsDisabled
		}
	default:
		// Treat as dot-notation content key.
		applyContentKeyOverride(local.Content, remote.Content, fieldName, direction)
	}
}

func applyContentKeyOverride(localContent, remoteContent *orderedmap.OrderedMap, fieldPath string, direction SyncDirection) {
	if localContent == nil || remoteContent == nil {
		return
	}
	if direction == SyncDirectionPush {
		// Keep remote: copy remote value → local.
		value, found, _ := remoteContent.GetNested(fieldPath)
		if !found {
			return
		}
		_ = localContent.SetNested(fieldPath, value)
	} else {
		// Keep local: copy local value → remote.
		value, found, _ := localContent.GetNested(fieldPath)
		if !found {
			return
		}
		_ = remoteContent.SetNested(fieldPath, value)
	}
}
