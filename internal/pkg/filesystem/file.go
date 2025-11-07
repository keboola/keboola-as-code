package filesystem

import (
	"strings"

	jsonnetast "github.com/google/go-jsonnet/ast"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/yaml"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type FileLine struct {
	Line   string
	Regexp string
}

type FileType int

const (
	FileTypeRaw     FileType = iota // RawFile
	FileTypeJSON                    // JSONFile
	FileTypeYaml                    // YamlFile
	FileTypeJsonnet                 // JsonnetFile
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

func (f *RawFile) ToJSONFile() (*JSONFile, error) {
	jsonMap := orderedmap.New()
	if err := json.DecodeString(f.Content, jsonMap); err != nil {
		fileDesc := formatFileDescription(f.desc)
		return nil, errors.PrefixErrorf(err, `%s "%s" is invalid`, fileDesc, f.path)
	}

	file := NewJSONFile(f.path, jsonMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *RawFile) ToYamlFile() (*YamlFile, error) {
	yamlMap := orderedmap.New()
	if err := yaml.DecodeString(f.Content, yamlMap); err != nil {
		fileDesc := formatFileDescription(f.desc)
		return nil, errors.PrefixErrorf(err, `%s "%s" is invalid`, fileDesc, f.path)
	}

	file := NewYamlFile(f.path, yamlMap)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *RawFile) ToJSONNetFile(ctx *jsonnet.Context) (*JsonnetFile, error) {
	ast, err := jsonnet.ToAst(f.Content, f.path)
	if err != nil {
		return nil, err
	}

	file := NewJsonnetFile(f.path, ast, ctx)
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

type JSONFile struct {
	*FileDef
	Content *orderedmap.OrderedMap
}

func NewJSONFile(path string, content *orderedmap.OrderedMap) *JSONFile {
	file := &JSONFile{FileDef: NewFileDef(path)}
	file.Content = content
	return file
}

func (f *JSONFile) Description() string {
	return f.desc
}

func (f *JSONFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *JSONFile) Path() string {
	return f.path
}

func (f *JSONFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JSONFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *JSONFile) ToRawFile() (*RawFile, error) {
	content, err := json.EncodeString(f.Content, true)
	if err != nil {
		fileDesc := formatFileDescription(f.desc)
		return nil, errors.PrefixErrorf(err, "cannot encode %s \"%s\"", fileDesc, f.path)
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JSONFile) ToJsonnetFile() (*JsonnetFile, error) {
	fileRaw, err := f.ToRawFile()
	if err != nil {
		return nil, err
	}
	fileRaw.SetPath(strings.TrimSuffix(f.path, `.json`) + `.jsonnet`)
	// ctx = nil: Jsonnet created from the Json cannot contain variables
	return fileRaw.ToJSONNetFile(nil)
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
		fileDesc := formatFileDescription(f.desc)
		return nil, errors.PrefixErrorf(err, "cannot encode %s \"%s\"", fileDesc, f.path)
	}

	file := NewRawFile(f.path, content)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

type JsonnetFile struct {
	*FileDef
	context *jsonnet.Context
	Content jsonnetast.Node
}

func NewJsonnetFile(path string, content jsonnetast.Node, ctx *jsonnet.Context) *JsonnetFile {
	return &JsonnetFile{FileDef: NewFileDef(path), context: ctx, Content: content}
}

func (f *JsonnetFile) SetContext(ctx *jsonnet.Context) {
	f.context = ctx
}

func (f *JsonnetFile) Description() string {
	return f.desc
}

func (f *JsonnetFile) SetDescription(desc string) File {
	f.desc = desc
	return f
}

func (f *JsonnetFile) Path() string {
	return f.path
}

func (f *JsonnetFile) AddTag(tags ...string) File {
	f.FileDef.AddTag(tags...)
	return f
}

func (f *JsonnetFile) RemoveTag(tags ...string) File {
	f.FileDef.RemoveTag(tags...)
	return f
}

func (f *JsonnetFile) ToJSONFile() (*JSONFile, error) {
	fileRaw, err := f.ToJSONRawFile()
	if err != nil {
		return nil, err
	}
	return fileRaw.ToJSONFile()
}

func (f *JsonnetFile) ToJSONRawFile() (*RawFile, error) {
	jsonContent, err := jsonnet.EvaluateAst(f.Content, f.context)
	if err != nil {
		return nil, err
	}

	file := NewRawFile(f.path, jsonContent)
	file.SetDescription(f.desc)
	file.AddTag(f.AllTags()...)
	return file, nil
}

func (f *JsonnetFile) ToRawFile() (*RawFile, error) {
	jsonContent, err := jsonnet.FormatNode(f.Content)
	if err != nil {
		return nil, err
	}

	file := NewRawFile(f.path, jsonContent)
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

func formatFileDescription(fileDesc string) string {
	return strings.TrimSpace(fileDesc + " file")
}
