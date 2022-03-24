package manifest

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

// Manifest is common interface for Project and Template manifest.
type Manifest interface {
	Path() string
	Sorter() model.ObjectsSorter
	NamingTemplate() naming.Template
	NamingRegistry() *naming.Registry
	All() []model.ObjectManifest
	Get(key model.Key) (model.ObjectManifest, bool)
	Set(records []model.ObjectManifest) error
	Add(records ...model.ObjectManifest) error
	Remove(keys ...model.Key)
}
