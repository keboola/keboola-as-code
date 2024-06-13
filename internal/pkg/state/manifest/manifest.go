package manifest

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

// Manifest is common interface form Project and Template manifest.
type Manifest interface {
	Path() string
	All() []model.ObjectManifest
	AllPersisted() []model.ObjectManifest
	SortBy() string
	NamingTemplate() naming.Template
	NamingRegistry() *naming.Registry
	ResolveParentPath(record model.ObjectManifest) error
	GetRecord(key model.Key) (model.ObjectManifest, bool)
	CreateOrGetRecord(key model.Key) (record model.ObjectManifest, found bool, err error)
	PersistRecord(manifest model.ObjectManifest) error
	Delete(object model.WithKey)
}
