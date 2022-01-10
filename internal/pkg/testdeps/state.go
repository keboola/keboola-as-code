package testdeps

import (
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func NewEmptyState(d *TestContainer, manifest manifest.Manifest) *state.State {
	s, err := state.New(NewObjectsContainer(d.Fs(), manifest), d)
	if err != nil {
		panic(err)
	}
	return s
}
