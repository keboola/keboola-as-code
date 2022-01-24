package testdeps

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func NewEmptyState(d *TestContainer, manifest manifest.Manifest, variables *fileloader.Variables) (*state.State, error) {
	fs := d.Fs()
	s, err := state.New(NewObjectsContainer(fs, manifest, variables), d)
	if err != nil {
		panic(err)
	}
	return s, nil
}
