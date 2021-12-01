package persist

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type action interface {
	Order() int
	String() string
	Path() string
}

type newObjectAction struct {
	model.PathInProject
	Key       model.Key
	ParentKey model.Key
	OnPersist []func(key model.Key)
}

type deleteManifestRecordAction struct {
	model.ObjectManifest
}

func (a *newObjectAction) Order() int {
	return 1
}

func (a *deleteManifestRecordAction) Order() int {
	return 2
}

func (a *newObjectAction) String() string {
	return fmt.Sprintf(`+ %s %s`, a.Key.Kind().Abbr, a.Path())
}

func (a *newObjectAction) InvokeOnPersist(key model.Key) {
	for _, callback := range a.OnPersist {
		callback(key)
	}
}

func (a *deleteManifestRecordAction) String() string {
	return fmt.Sprintf(`- %s %s`, a.Kind().Abbr, a.Path())
}
