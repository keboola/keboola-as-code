package manifest

import (
	"encoding/json"
	"fmt"
	"github.com/go-playground/validator/v10"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

const (
	FileName = "manifest.json"
)

type Manifest struct {
	path           string
	Version        int              `json:"version" validate:"required,min=1,max=1"`
	Project        *Project         `json:"project" validate:"required"`
	Branches       []*Branch        `json:"branches"`
	Configurations []*Configuration `json:"configurations"`
}

type Project struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type Branch struct {
	Id   int    `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type Configuration struct {
	Id          int    `json:"id" validate:"required,min=1"`
	ComponentId string `json:"componentId" validate:"required"`
	BranchId    int    `json:"branchId" validate:"required"`
	Path        string `json:"path" validate:"required"`
	Rows        []*Row `json:"rows"`
}

type Row struct {
	Id   int    `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

func NewManifest(projectId int, apiHost string) (*Manifest, error) {
	m := &Manifest{
		Version:        1,
		Project:        &Project{Id: projectId, ApiHost: apiHost},
		Branches:       make([]*Branch, 0),
		Configurations: make([]*Configuration, 0),
	}
	err := m.Validate()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func Load(metadataDir string) (*Manifest, error) {
	// Load file
	path := filepath.Join(metadataDir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decode JSON
	m := &Manifest{}
	err = json.Unmarshal(data, m)
	if err != nil {
		return nil, processJsonError(err)
	}

	// Validate
	err = m.Validate()
	if err != nil {
		return nil, err
	}

	// Set path
	m.path = path

	// Return
	return m, nil
}

func (m *Manifest) Save(metadataDir string) error {
	// Validate
	err := m.Validate()
	if err != nil {
		return err
	}

	// Encode JSON
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return processJsonError(err)
	}

	// Write file
	m.path = filepath.Join(metadataDir, FileName)
	return os.WriteFile(m.path, data, 0650)
}

func (m *Manifest) Validate() error {
	// Setup
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		// Use JSON field name in error messages
		return strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
	})

	// Do
	if err := validate.Struct(m); err != nil {
		return processValidateError(err.(validator.ValidationErrors))
	}
	return nil
}

func (m *Manifest) Path() string {
	if len(m.path) == 0 {
		panic(fmt.Errorf("path is not set"))
	}
	return m.path
}

func processJsonError(err error) error {
	result := &Error{}

	switch err := err.(type) {
	// Custom error message
	case *json.UnmarshalTypeError:
		result.Add(fmt.Errorf("key \"%s\" has invalid type \"%s\"", err.Field, err.Value))
	default:
		result.Add(err)
	}

	return result
}

func processValidateError(err validator.ValidationErrors) error {
	result := &Error{}
	for _, e := range err {
		path := strings.TrimPrefix(e.Namespace(), "Manifest.")
		result.Add(fmt.Errorf(
			"key=\"%s\", value=\"%v\", failed \"%s\" validation",
			path,
			e.Value(),
			e.ActualTag(),
		))
	}

	// Convert msg to error
	if result.Len() > 0 {
		return result
	}
	return nil

}
