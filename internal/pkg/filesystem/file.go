package filesystem

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type FileLine struct {
	Line   string
	Regexp string
}

type File struct {
	Content string
	Desc    string
	Path    string
}

type JsonFile struct {
	Content *orderedmap.OrderedMap
	Desc    string
	Path    string
}

func CreateFile(path, content string) *File {
	file := &File{}
	file.Path = path
	file.Content = content
	return file
}

func CreateJsonFile(path string, content *orderedmap.OrderedMap) *JsonFile {
	file := &JsonFile{}
	file.Path = path
	file.Content = content
	return file
}

func (f *File) SetDescription(desc string) *File {
	f.Desc = desc
	return f
}

func (f *File) ToJsonFile() (*JsonFile, error) {
	m := utils.NewOrderedMap()
	if err := json.DecodeString(f.Content, m); err != nil {
		fileDesc := strings.TrimSpace(f.Desc + " file")
		return nil, utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", fileDesc, f.Path), err)
	}

	file := &JsonFile{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = m
	return file, nil
}

func (f *JsonFile) SetDescription(desc string) *JsonFile {
	f.Desc = desc
	return f
}

func (f *JsonFile) ToFile() (*File, error) {
	content, err := json.EncodeString(f.Content, true)
	if err != nil {
		fileDesc := strings.TrimSpace(f.Desc + " file")
		return nil, utils.PrefixError(fmt.Sprintf("cannot encode %s \"%s\"", fileDesc, f.Path), err)
	}

	file := &File{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = content
	return file, nil
}
