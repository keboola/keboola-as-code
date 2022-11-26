package fileloader

import (
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/yaml"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/reflecthelper"
)

const (
	KbcDirFileName  = "kbcdir.json"
	KbcDirIsIgnored = "isIgnored"
)

// loadHandlerWithNext callback modifies file loading process.
// In addition to filesystem.LoadHandler, it contains reference to the "next" handler.
type loadHandlerWithNext func(def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error)

// loader implements filesystem.FileLoader.
type loader struct {
	fs             filesystem.Fs
	handler        loadHandlerWithNext
	jsonNetContext *jsonnet.Context
}

// New creates FileLoader to load files from the filesystem.
func New(fs filesystem.Fs) filesystem.FileLoader {
	return &loader{fs: fs, jsonNetContext: jsonnet.NewContext().WithImporter(fsimporter.New(fs))}
}

// NewWithHandler creates FileLoader to load files from the filesystem.
// File load process can be modified by the custom handler callback.
func NewWithHandler(fs filesystem.Fs, handler loadHandlerWithNext) filesystem.FileLoader {
	return &loader{fs: fs, handler: handler, jsonNetContext: jsonnet.NewContext().WithImporter(fsimporter.New(fs))}
}

func (l *loader) WithJSONNETContext(ctx *jsonnet.Context) filesystem.FileLoader {
	clone := *l
	clone.jsonNetContext = ctx
	return &clone
}

// ReadRawFile - file content is loaded as a string.
func (l *loader) ReadRawFile(def *filesystem.FileDef) (*filesystem.RawFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeRaw)
	if err != nil {
		return nil, err
	}

	// Convert to RawFile
	if f, ok := file.(*filesystem.RawFile); ok {
		return f, nil
	}
	return file.ToRawFile()
}

// ReadFileContentTo to tagged field in target struct as string.
func (l *loader) ReadFileContentTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.RawFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadRawFile(def); err == nil {
			content := strings.TrimRight(file.Content, " \r\n\t")
			reflecthelper.SetField(field, content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadJSONFile as an ordered map.
func (l *loader) ReadJSONFile(def *filesystem.FileDef) (*filesystem.JSONFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeJSON)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JSONFile), nil
}

// ReadJSONFileTo to the target struct.
func (l *loader) ReadJSONFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.RawFile, error) {
	file, err := l.ReadRawFile(def)
	if err != nil {
		return nil, err
	}

	if err := json.DecodeString(file.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadJSONFieldsTo tagged fields in the target struct.
func (l *loader) ReadJSONFieldsTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.JSONFile, bool, error) {
	if fields := reflecthelper.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadJSONFile(def); err == nil {
			reflecthelper.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadJSONMapTo tagged field in the target struct as ordered map.
func (l *loader) ReadJSONMapTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.JSONFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadJSONFile(def); err == nil {
			reflecthelper.SetField(field, file.Content, target)
			return file, true, nil
		} else {
			// Set empty map if error occurred
			reflecthelper.SetField(field, orderedmap.New(), target)
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadYamlFile as an ordered map.
func (l *loader) ReadYamlFile(def *filesystem.FileDef) (*filesystem.YamlFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeYaml)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.YamlFile), nil
}

// ReadYamlFileTo to the target struct.
func (l *loader) ReadYamlFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.RawFile, error) {
	file, err := l.ReadRawFile(def)
	if err != nil {
		return nil, err
	}

	if err := yaml.DecodeString(file.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadYamlFieldsTo tagged fields in the target struct.
func (l *loader) ReadYamlFieldsTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.YamlFile, bool, error) {
	if fields := reflecthelper.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadYamlFile(def); err == nil {
			reflecthelper.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadYamlMapTo tagged field in the target struct as ordered map.
func (l *loader) ReadYamlMapTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.YamlFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadYamlFile(def); err == nil {
			reflecthelper.SetField(field, file.Content, target)
			return file, true, nil
		} else {
			// Set empty map if error occurred
			reflecthelper.SetField(field, orderedmap.New(), target)
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadJSONNETFile as AST.
func (l *loader) ReadJSONNETFile(def *filesystem.FileDef) (*filesystem.JSONNETFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeJSONNET)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JSONNETFile), nil
}

// ReadJSONNETFileTo the target struct.
func (l *loader) ReadJSONNETFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.JSONNETFile, error) {
	jsonNetFile, err := l.ReadJSONNETFile(def)
	if err != nil {
		return nil, formatFileError(def, err)
	}

	jsonFile, err := jsonNetFile.ToJSONRawFile()
	if err != nil {
		return nil, formatFileError(def, err)
	}

	if err := json.DecodeString(jsonFile.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return jsonNetFile, nil
}

// ReadSubDirs filter out ignored directories.
func (l *loader) ReadSubDirs(fs filesystem.Fs, root string) ([]string, error) {
	subDirs, err := filesystem.ReadSubDirs(fs, root)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, subDir := range subDirs {
		isIgnored, err := l.IsIgnored(filesystem.Join(root, subDir))
		if err != nil {
			return nil, err
		}
		if !isIgnored {
			res = append(res, subDir)
		}
	}
	return res, nil
}

// IsIgnored checks if the dir is ignored.
func (l *loader) IsIgnored(path string) (bool, error) {
	if !l.fs.IsDir(path) {
		return false, nil
	}
	fileDef := filesystem.NewFileDef(filesystem.Join(path, KbcDirFileName))
	fileDef.AddTag(`json`) // `json` is a constant model.FileTypeJSON but cannot be imported due to cyclic imports, it will be refactored

	file, err := l.ReadJSONFile(fileDef)
	if err != nil {
		if errors.Is(err, filesystem.ErrNotExist) {
			return false, err
		}
		return false, nil
	}
	isIgnored, found := file.Content.Get(KbcDirIsIgnored)
	if found {
		if isIgnored.(bool) {
			return true, nil
		}
	}

	return false, nil
}

func formatFileError(def *filesystem.FileDef, err error) error {
	fileDesc := strings.TrimSpace(def.Description() + " file")
	return errors.PrefixErrorf(err, `%s "%s" is invalid`, fileDesc, def.Path())
}

func (l *loader) loadFile(def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
	if l.handler != nil {
		return l.handler(def, fileType, l.defaultHandler)
	}
	return l.defaultHandler(def, fileType)
}

func (l *loader) defaultHandler(def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
	// Load
	rawFile, err := l.fs.ReadFile(def)
	if err != nil {
		return nil, err
	}

	// Convert
	switch fileType {
	case filesystem.FileTypeRaw:
		return rawFile, nil
	case filesystem.FileTypeJSON:
		return rawFile.ToJSONFile()
	case filesystem.FileTypeYaml:
		return rawFile.ToYamlFile()
	case filesystem.FileTypeJSONNET:
		return rawFile.ToJSONNetFile(l.jsonNetContext)
	default:
		panic(errors.Errorf(`unexpected filesystem.FileType = %v`, fileType))
	}
}
