package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PersistAction interface {
	String() string
}

type NewConfigAction struct {
	Key         model.ConfigKey
	Path        string
	ProjectPath string
	OnPersist   []func(key model.ConfigKey)
}

type NewRowAction struct {
	Key         model.ConfigRowKey
	Path        string
	ProjectPath string
}

type DeleteRecordAction struct {
	Record model.Record
}

func (a *NewConfigAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.ProjectPath)
}

func (a *NewRowAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.ProjectPath)
}

func (a *DeleteRecordAction) String() string {
	return fmt.Sprintf(`- %s %s`, a.Record.Kind().Abbr, a.Record.Path())
}

func (a *NewConfigAction) InvokeOnPersist(key model.ConfigKey) {
	for _, callback := range a.OnPersist {
		callback(key)
	}
}
