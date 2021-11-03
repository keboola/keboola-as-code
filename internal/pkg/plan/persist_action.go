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

type NewObjectAction struct {
	model.PathInProject
	Key       model.Key
	ParentKey model.Key
	OnPersist []func(key model.Key)
}

type DeleteRecordAction struct {
	model.Record
}

func (a *NewObjectAction) Order() int {
	return 1
}

func (a *DeleteRecordAction) Order() int {
	return 2
}

func (a *NewObjectAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.Path())
}

func (a *NewObjectAction) InvokeOnPersist(key model.Key) {
	for _, callback := range a.OnPersist {
		callback(key)
	}
}

func (a *DeleteRecordAction) String() string {
	return fmt.Sprintf(`- %s %s`, a.Kind().Abbr, a.Path())
}
