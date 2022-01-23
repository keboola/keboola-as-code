package filesystem

import (
	"fmt"
	"strings"

	jsonnetast "github.com/google/go-jsonnet/ast"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
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

type JsonNetFile struct {
	Content jsonnetast.Node
	Desc    string
	Path    string
}

// FileWrapper is anything that can be converted to File.
type FileWrapper interface {
	GetDescription() string
	GetPath() string
	ToFile() (*File, error)
}

func NewFile(path, content string) *File {
	file := &File{}
	file.Path = path
	file.Content = content
	return file
}

func NewJsonFile(path string, content *orderedmap.OrderedMap) *JsonFile {
	file := &JsonFile{}
	file.Path = path
	file.Content = content
	return file
}

func NewJsonNetFile(path string, content jsonnetast.Node) *JsonNetFile {
	file := &JsonNetFile{}
	file.Path = path
	file.Content = content
	return file
}

func (f *File) GetDescription() string {
	return f.Desc
}

func (f *File) SetDescription(desc string) *File {
	f.Desc = desc
	return f
}

func (f *File) GetPath() string {
	return f.Path
}

func (f *File) ToFile() (*File, error) {
	return f, nil
}

func (f *File) ToJsonFile() (*JsonFile, error) {
	m := orderedmap.New()
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

func (f *JsonFile) GetDescription() string {
	return f.Desc
}

func (f *JsonFile) SetDescription(desc string) *JsonFile {
	f.Desc = desc
	return f
}

func (f *JsonFile) GetPath() string {
	return f.Path
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

func (f *JsonFile) ToJsonNetFile() (*JsonNetFile, error) {
	jsonContent, err := json.EncodeString(f.Content, true)
	if err != nil {
		return nil, err
	}

	ast, err := jsonnet.ToAst(jsonContent)
	if err != nil {
		return nil, err
	}

	file := &JsonNetFile{}
	file.Path = strings.TrimSuffix(f.Path, `.json`) + `.jsonnet`
	file.Desc = f.Desc
	file.Content = ast
	return file, nil
}

func (f *JsonNetFile) GetDescription() string {
	return f.Desc
}

func (f *JsonNetFile) SetDescription(desc string) *JsonNetFile {
	f.Desc = desc
	return f
}

func (f *JsonNetFile) GetPath() string {
	return f.Path
}

func (f *JsonNetFile) ToJsonFile() (*JsonFile, error) {
	jsonContent, err := jsonnet.EvaluateAst(f.Content)
	if err != nil {
		return nil, err
	}

	jsonMap := orderedmap.New()
	if err := json.DecodeString(jsonContent, jsonMap); err != nil {
		return nil, err
	}

	file := &JsonFile{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = jsonMap
	return file, nil
}

func (f *JsonNetFile) ToFile() (*File, error) {
	content, err := jsonnet.FormatAst(f.Content)
	if err != nil {
		return nil, err
	}

	file := &File{}
	file.Path = f.Path
	file.Desc = f.Desc
	file.Content = content
	return file, nil
}
