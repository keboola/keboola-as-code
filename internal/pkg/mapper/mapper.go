package mapper

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type BeforeLocalSaveMapper interface {
	BeforeLocalSave(recipe *model.LocalSaveRecipe) error
}

type AfterLocalLoadMapper interface {
	AfterLocalLoad(recipe *model.LocalLoadRecipe) error
}

type BeforeRemoteSaveMapper interface {
	BeforeRemoteSave(recipe *model.RemoteSaveRecipe) error
}

type AfterRemoteLoadMapper interface {
	AfterRemoteLoad(recipe *model.RemoteLoadRecipe) error
}

type Mapper struct {
	context model.MapperContext
	mappers []interface{} // implement part of the interfaces above
}

func New(logger *zap.SugaredLogger, fs filesystem.Fs, naming *model.Naming, state *model.State) *Mapper {
	m := &Mapper{
		context: model.MapperContext{Logger: logger, Fs: fs, Naming: naming, State: state},
	}

	// Mappers
	m.mappers = append(
		m.mappers,
		sharedcode.NewMapper(m.context),
		transformation.NewMapper(m.context),
	)

	return m
}

func (m *Mapper) BeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(BeforeLocalSaveMapper); ok {
			if err := mapper.BeforeLocalSave(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) AfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(AfterLocalLoadMapper); ok {
			if err := mapper.AfterLocalLoad(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) BeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(BeforeRemoteSaveMapper); ok {
			if err := mapper.BeforeRemoteSave(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) AfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(AfterRemoteLoadMapper); ok {
			if err := mapper.AfterRemoteLoad(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}
