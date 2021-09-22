package model

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type FileLine struct {
	Line   string
	Regexp string
}

// FilePath relative to the project dir.
type FilePath string

type File struct {
	Content string
	Desc    string
	Path    FilePath
}

type JsonFile struct {
	Content *orderedmap.OrderedMap
	Desc    string
	Path    FilePath
}

// ObjectFiles - all files related to the object, when saving.
type ObjectFiles struct {
	Record        Record
	Object        Object
	Metadata      *JsonFile  // meta.json
	Description   *File      // description.md
	Configuration *JsonFile  // config.json
	Extra         []*File    // extra files
	ToDelete      []FilePath // files to delete, on save
}

func CreateFile(path, desc, content string) *File {
	if len(desc) == 0 {
		desc = "file"
	}
	file := &File{}
	file.Path = FilePath(path)
	file.Desc = desc
	file.Content = content
	return file
}

func CreateJsonFile(path, desc string, content *orderedmap.OrderedMap) *JsonFile {
	file := &JsonFile{}
	file.Path = FilePath(path)
	file.Desc = desc
	file.Content = content
	return file
}

func (f *JsonFile) ToFile() (*File, error) {
	content, err := json.EncodeString(f.Content, true)
	if err != nil {
		return nil, utils.PrefixError(fmt.Sprintf("cannot encode %s \"%s\"", f.Desc, f.Path), err)
	}

	file := &File{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = content
	return file, err
}

func (f *File) ToJsonFile() (*JsonFile, error) {
	m := utils.NewOrderedMap()
	if err := json.DecodeString(f.Content, m); err != nil {
		return nil, utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", f.Desc, f.Path), err)
	}

	file := &JsonFile{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = m
	return file, nil
}
