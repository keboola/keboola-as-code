package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PersistAction interface {
	String() string
	Path() string
}

type NewConfigAction struct {
	model.PathInProject
	Key       model.ConfigKey
	OnPersist []func(key model.ConfigKey)
}

type NewRowAction struct {
	model.PathInProject
	Key model.ConfigRowKey
}

type DeleteRecordAction struct {
	model.Record
}

func (a *NewConfigAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.Path())
}

func (a *NewRowAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.Path())
}

func (a *DeleteRecordAction) String() string {
	return fmt.Sprintf(`- %s %s`, a.Record.Kind().Abbr, a.Record.Path())
}

func (a *NewConfigAction) InvokeOnPersist(key model.ConfigKey) {
	for _, callback := range a.OnPersist {
		callback(key)
	}
}
