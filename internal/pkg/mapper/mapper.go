package mapper

import (
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

func New(context model.MapperContext) *Mapper {
	return &Mapper{context: context}
}

func (m *Mapper) AddMapper(mapper ...interface{}) *Mapper {
	m.mappers = append(m.mappers, mapper...)
	return m
}

func (m *Mapper) Context() model.MapperContext {
	return m.context
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
