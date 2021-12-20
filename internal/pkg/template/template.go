package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type Manifest = templateManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return templateManifest.Load(fs)
}

type Template struct {
	fs       filesystem.Fs
	manifest *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest) *Template {
	return &Template{
		fs:       fs,
		manifest: manifest,
	}
}

func (p *Template) Fs() filesystem.Fs {
	return p.fs
}

func (p *Template) Manifest() manifest.Manifest {
	return p.manifest
}

// --- move next code - ReplacesKeys to mapper

type template struct {
	objects []model.Object
}

func FromState(objects model.ObjectStates, stateType model.StateType) *template {
	return &template{objects: objectFromState(objects, stateType)}
}

func (t *template) ReplaceKeys(keys KeysReplacement) error {
	values, err := keys.Values()
	if err != nil {
		return err
	}
	t.objects = replaceValues(values, t.objects).([]model.Object)
	return nil
}

func objectFromState(allObjects model.ObjectStates, stateType model.StateType) []model.Object {
	all := allObjects.ObjectsInState(stateType).All()
	objects := make([]model.Object, len(all))
	copy(objects, all)
	return objects
}
