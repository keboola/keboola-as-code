package filesystem

import (
	"strings"

	jsonnetast "github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/yaml"
)

type FileLine struct {
	Line   string
	Regexp string
}

type FileType int

const (
	FileTypeRaw     FileType = iota // RawFile
	FileTypeJson                    // JsonFile
	FileTypeYaml                    // YamlFile
	FileTypeJsonNet                 // JsonNetFile
)

const (
	ObjectKeyMetadata = "objectKey"
)

type FileDef struct {
	*FileTags
	*FileMetadata
	desc string
	path string
}

type RawFile struct {
	*FileDef
	Content string
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
	return &FileDef{FileTags: NewFileTags(), FileMetadata: NewFileMetadata(), path: path}
}

func (f *FileDef) Path() string {
	return f.path
}

func (f *FileDef) SetPath(v string) *FileDef {
	f.path = v
	return f
}

func (f *FileDef) Description() string {
	return f.desc
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

// ToEmptyFile converts FileDef to a File with empty content.
func (f *FileDef) ToEmptyFile() *RawFile {
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
		return nil, errors.PrefixErrorf(err, `%s "%s" is invalid`, fileDesc, f.path)
	}

	file := NewJsonFile(f.path, jsonMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *RawFile) ToYamlFile() (*YamlFile, error) {
	yamlMap := orderedmap.New()
	if err := yaml.DecodeString(f.Content, yamlMap); err != nil {
		fileDesc := strings.TrimSpace(f.desc + " file")
		return nil, errors.PrefixErrorf(err, `%s "%s" is invalid`, fileDesc, f.path)
	}

	file := NewYamlFile(f.path, yamlMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *RawFile) ToJsonNetFile(ctx *jsonnet.Context) (*JsonNetFile, error) {
	ast, err := jsonnet.ToAst(f.Content, f.path)
	if err != nil {
		return nil, err
	}

	file := NewJsonNetFile(f.path, ast, ctx)
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

type JsonFile struct {
	*FileDef
	Content *orderedmap.OrderedMap
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

func (f *JsonFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JsonFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *JsonFile) ToRawFile() (*RawFile, error) {
	content, err := json.EncodeString(f.Content, true)
	if err != nil {
		fileDesc := strings.TrimSpace(f.desc + " file")
		return nil, errors.PrefixErrorf(err, "cannot encode %s \"%s\"", fileDesc, f.path)
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JsonFile) ToJsonNetFile() (*JsonNetFile, error) {
	fileRaw, err := f.ToRawFile()
	if err != nil {
		return nil, err
	}
	fileRaw.SetPath(strings.TrimSuffix(f.path, `.json`) + `.jsonnet`)
	// ctx = nil: JsonNet created from the Json cannot contain variables
	return fileRaw.ToJsonNetFile(nil)
}

type YamlFile struct {
	*FileDef
	Content *orderedmap.OrderedMap
}

func NewYamlFile(path string, content *orderedmap.OrderedMap) *YamlFile {
	file := &YamlFile{FileDef: NewFileDef(path)}
	file.Content = content
	return file
}

func (f *YamlFile) Description() string {
	return f.desc
}

func (f *YamlFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *YamlFile) Path() string {
	return f.path
}

func (f *YamlFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *YamlFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *YamlFile) ToRawFile() (*RawFile, error) {
	content, err := yaml.EncodeString(f.Content)
	if err != nil {
		fileDesc := strings.TrimSpace(f.desc + " file")
		return nil, errors.PrefixErrorf(err, "cannot encode %s \"%s\"", fileDesc, f.path)
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

type JsonNetFile struct {
	*FileDef
	context *jsonnet.Context
	Content jsonnetast.Node
}

func NewJsonNetFile(path string, content jsonnetast.Node, ctx *jsonnet.Context) *JsonNetFile {
	return &JsonNetFile{FileDef: NewFileDef(path), context: ctx, Content: content}
}

func (f *JsonNetFile) SetContext(ctx *jsonnet.Context) {
	f.context = ctx
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

func (f *JsonNetFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JsonNetFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *JsonNetFile) ToJsonFile() (*JsonFile, error) {
	fileRaw, err := f.ToJsonRawFile()
	if err != nil {
		return nil, err
	}
	return fileRaw.ToJsonFile()
}

func (f *JsonNetFile) ToJsonRawFile() (*RawFile, error) {
	jsonContent, err := jsonnet.EvaluateAst(f.Content, f.context)
	if err != nil {
		return nil, err
	}

	file := NewRawFile(f.path, jsonContent)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JsonNetFile) ToRawFile() (*RawFile, error) {
	file := NewRawFile(f.path, jsonnet.FormatAst(f.Content))
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
	if files := f.GetByTag(tag); len(files) == 1 {
		return files[0]
	} else if len(files) > 1 {
		var paths []string
		for _, file := range files {
			paths = append(paths, file.Path())
		}
		panic(errors.Errorf(`found multiple files with tag "%s": "%s"`, tag, strings.Join(paths, `", "`)))
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
