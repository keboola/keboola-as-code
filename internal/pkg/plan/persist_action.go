package plan

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PersistAction interface {
	Order() int
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

type NewVariablesRelAction struct {
	Variables *model.ConfigKey
	ConfigKey *model.ConfigKey
}

type DeleteRecordAction struct {
	model.Record
}

func (a *NewConfigAction) Order() int {
	return 1
}

func (a *NewRowAction) Order() int {
	return 1
}

func (a *NewVariablesRelAction) Order() int {
	return 2
}

func (a *DeleteRecordAction) Order() int {
	return 3
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

func (a *NewVariablesRelAction) String() string {
	return ""
}

func (a *NewVariablesRelAction) Path() string {
	return ""
}

func (a *NewConfigAction) InvokeOnPersist(key model.ConfigKey) {
	for _, callback := range a.OnPersist {
		callback(key)
	}
}
