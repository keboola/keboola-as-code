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

type FileDef struct {
	*FileTags
	desc string
	path string
}

type RawFile struct {
	*FileDef
	Content string
}

type JsonFile struct {
	*FileDef
	Content *orderedmap.OrderedMap
}

type JsonNetFile struct {
	*FileDef
	Content jsonnetast.Node
}

type Files struct {
	files []File
}

// File is common abstraction for a file.
type File interface {
	Description() string
	SetDescription(v string) File
	Path() string
	ToRawFile() (*RawFile, error)
	AllTags() []string
	HasTag(tag string) bool
	AddTag(tags ...string) File
	RemoveTag(tags ...string) File
}

func NewFileDef(path string) *FileDef {
	return &FileDef{FileTags: NewFileTags(), path: path}
}

func (f *FileDef) SetDescription(v string) *FileDef {
	f.desc = v
	return f
}

func NewRawFile(path, content string) *RawFile {
	file := &RawFile{FileDef: NewFileDef(path)}
	file.Content = content
	return file
}

// ToFile converts FileDef to a File with empty content.
func (f *FileDef) ToFile() *RawFile {
	file := NewRawFile(f.path, "")
	file.desc = f.desc
	file.AddTag(f.AllTags()...)
	return file
}

func (f *RawFile) Description() string {
	return f.desc
}

func (f *RawFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *RawFile) Path() string {
	return f.path
}

func (f *RawFile) ToRawFile() (*RawFile, error) {
	return f, nil
}

func (f *RawFile) ToJsonFile() (*JsonFile, error) {
	jsonMap := orderedmap.New()
	if err := json.DecodeString(f.Content, jsonMap); err != nil {
		fileDesc := strings.TrimSpace(f.desc + " file")
		return nil, utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", fileDesc, f.path), err)
	}

	file := NewJsonFile(f.path, jsonMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *RawFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *RawFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func NewJsonFile(path string, content *orderedmap.OrderedMap) *JsonFile {
	file := &JsonFile{FileDef: NewFileDef(path)}
	file.Content = content
	return file
}

func (f *JsonFile) Description() string {
	return f.desc
}

func (f *JsonFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *JsonFile) Path() string {
	return f.path
}

func (f *JsonFile) ToRawFile() (*RawFile, error) {
	content, err := json.EncodeString(f.Content, true)
	if err != nil {
		fileDesc := strings.TrimSpace(f.desc + " file")
		return nil, utils.PrefixError(fmt.Sprintf("cannot encode %s \"%s\"", fileDesc, f.path), err)
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JsonFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JsonFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
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

	path := strings.TrimSuffix(f.path, `.json`) + `.jsonnet`
	file := NewJsonNetFile(path, ast)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func NewJsonNetFile(path string, content jsonnetast.Node) *JsonNetFile {
	file := &JsonNetFile{FileDef: NewFileDef(path)}
	file.Content = content
	return file
}

func (f *JsonNetFile) Description() string {
	return f.desc
}

func (f *JsonNetFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *JsonNetFile) Path() string {
	return f.path
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

	file := NewJsonFile(f.path, jsonMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JsonNetFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JsonNetFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *JsonNetFile) ToRawFile() (*RawFile, error) {
	content, err := jsonnet.FormatAst(f.Content)
	if err != nil {
		return nil, err
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func NewFiles() *Files {
	return &Files{}
}

func (f *Files) All() []File {
	out := make([]File, len(f.files))
	copy(out, f.files)
	return out
}

func (f *Files) Add(file File) File {
	f.files = append(f.files, file)
	return file
}

func (f *Files) GetOneByTag(tag string) File {
	files := f.GetByTag(tag)
	if len(files) == 1 {
		return files[0]
	} else if len(files) > 1 {
		var paths []string
		for _, file := range files {
			paths = append(paths, file.Path())
		}
		panic(fmt.Errorf(`found multiple files with tag "%s": "%s"`, tag, strings.Join(paths, `", "`)))
	}
	return nil
}

func (f *Files) GetByTag(tag string) []File {
	var out []File
	for _, file := range f.files {
		if file.HasTag(tag) {
			out = append(out, file)
		}
	}
	return out
}
