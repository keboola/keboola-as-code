package local

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Manager struct {
	logger   *zap.SugaredLogger
	fs       filesystem.Fs
	manifest *manifest.Manifest
	state    *model.State
	mapper   *mapper.Mapper
}

func NewManager(logger *zap.SugaredLogger, fs filesystem.Fs, m *manifest.Manifest, state *model.State) *Manager {
	return &Manager{
		logger:   logger,
		fs:       fs,
		manifest: m,
		state:    state,
		mapper:   mapper.New(logger, fs, m.Naming, state),
	}
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.manifest
}

func (m *Manager) Naming() *model.Naming {
	return m.manifest.Naming
}
