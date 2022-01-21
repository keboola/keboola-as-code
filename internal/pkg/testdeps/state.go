package testdeps

import (
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func NewEmptyState(d *TestContainer, manifest manifest.Manifest) (*state.State, error) {
	fs := d.Fs()
	s, err := state.New(NewObjectsContainer(fs, manifest), d)
	if err != nil {
		panic(err)
	}
	return s, nil
}
