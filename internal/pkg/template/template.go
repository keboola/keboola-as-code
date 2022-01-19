package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

type (
	Manifest = templateManifest.Manifest
	Inputs   = templateInput.Inputs
)

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return templateManifest.Load(fs)
}

type dependencies interface {
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
}

type Template struct {
	dependencies
	fs       filesystem.Fs
	manifest *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest, d dependencies) *Template {
	return &Template{
		dependencies: d,
		fs:           fs,
		manifest:     manifest,
	}
}

func (p *Template) Fs() filesystem.Fs {
	return p.fs
}

func (p *Template) Manifest() manifest.Manifest {
	return p.manifest
}

func (p *Template) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, p.dependencies)
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
