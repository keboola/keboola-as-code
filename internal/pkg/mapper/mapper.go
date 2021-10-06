package mapper

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type state = model.State

type Mapper struct {
	*state
	logger *zap.SugaredLogger
	fs     filesystem.Fs
	naming model.Naming
}

func New(state *model.State, logger *zap.SugaredLogger, fs filesystem.Fs, naming model.Naming) *Mapper {
	return &Mapper{state: state, logger: logger, fs: fs, naming: naming}
}

func (m *Mapper) BeforeSave(files *model.ObjectFiles) error {
	// Save transformation
	if ok, err := m.isTransformationConfig(files.Object); err != nil {
		return err
	} else if ok {
		return transformation.Save(
			m.logger,
			m.fs,
			m.naming,
			m.state,
			files,
		)
	}

	// Save shared code
	if ok, err := m.isSharedCodeConfigRow(files.Object); err != nil {
		return err
	} else if ok {
		return sharedcode.Save(
			m.logger,
			m.fs,
			m.naming,
			m.state,
			files,
		)
	}

	return nil
}

func (m *Mapper) AfterLoad(files *model.ObjectFiles) error {
	// Load transformation
	if ok, err := m.isTransformationConfig(files.Object); ok {
		return transformation.Load(
			m.logger,
			m.fs,
			m.naming,
			m.state,
			files,
		)
	} else if err != nil {
		return err
	}

	// Load shared code
	if ok, err := m.isSharedCodeConfigRow(files.Object); ok {
		return sharedcode.Load(
			m.logger,
			m.fs,
			m.naming,
			m.state,
			files,
		)
	} else if err != nil {
		return err
	}

	return nil
}

func (m *Mapper) isTransformationConfig(object interface{}) (bool, error) {
	v, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.Components().Get(*v.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsTransformation(), nil
}

func (m *Mapper) isSharedCodeConfigRow(object interface{}) (bool, error) {
	v, ok := object.(*model.ConfigRow)
	if !ok {
		return false, nil
	}

	component, err := m.Components().Get(*v.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsSharedCode(), nil
}
