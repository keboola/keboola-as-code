package fileloader

import (
	"context"
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
type loadHandlerWithNext func(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error)

// loader implements filesystem.FileLoader.
type loader struct {
	fs             filesystem.Fs
	handler        loadHandlerWithNext
	jsonnetContext *jsonnet.Context
}

// New creates FileLoader to load files from the filesystem.
func New(fs filesystem.Fs) filesystem.FileLoader {
	return &loader{fs: fs, jsonnetContext: jsonnet.NewContext().WithImporter(fsimporter.New(fs))}
}

// NewWithHandler creates FileLoader to load files from the filesystem.
// File load process can be modified by the custom handler callback.
func NewWithHandler(fs filesystem.Fs, handler loadHandlerWithNext) filesystem.FileLoader {
	return &loader{fs: fs, handler: handler, jsonnetContext: jsonnet.NewContext().WithImporter(fsimporter.New(fs))}
}

func (l *loader) WithJsonnetContext(ctx *jsonnet.Context) filesystem.FileLoader {
	clone := *l
	clone.jsonnetContext = ctx
	return &clone
}

// ReadRawFile - file content is loaded as a string.
func (l *loader) ReadRawFile(ctx context.Context, def *filesystem.FileDef) (*filesystem.RawFile, error) {
	file, err := l.loadFile(ctx, def, filesystem.FileTypeRaw)
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
func (l *loader) ReadFileContentTo(ctx context.Context, def *filesystem.FileDef, target any, tag string) (*filesystem.RawFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadRawFile(ctx, def); err == nil {
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
func (l *loader) ReadJSONFile(ctx context.Context, def *filesystem.FileDef) (*filesystem.JSONFile, error) {
	file, err := l.loadFile(ctx, def, filesystem.FileTypeJSON)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JSONFile), nil
}

// ReadJSONFileTo to the target struct.
func (l *loader) ReadJSONFileTo(ctx context.Context, def *filesystem.FileDef, target any) (*filesystem.RawFile, error) {
	file, err := l.ReadRawFile(ctx, def)
	if err != nil {
		return nil, err
	}

	if err := json.DecodeString(file.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadJSONFieldsTo tagged fields in the target struct.
func (l *loader) ReadJSONFieldsTo(ctx context.Context, def *filesystem.FileDef, target any, tag string) (*filesystem.JSONFile, bool, error) {
	if fields := reflecthelper.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadJSONFile(ctx, def); err == nil {
			reflecthelper.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadJSONMapTo tagged field in the target struct as ordered map.
func (l *loader) ReadJSONMapTo(ctx context.Context, def *filesystem.FileDef, target any, tag string) (*filesystem.JSONFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadJSONFile(ctx, def); err == nil {
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
func (l *loader) ReadYamlFile(ctx context.Context, def *filesystem.FileDef) (*filesystem.YamlFile, error) {
	file, err := l.loadFile(ctx, def, filesystem.FileTypeYaml)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.YamlFile), nil
}

// ReadYamlFileTo to the target struct.
func (l *loader) ReadYamlFileTo(ctx context.Context, def *filesystem.FileDef, target any) (*filesystem.RawFile, error) {
	file, err := l.ReadRawFile(ctx, def)
	if err != nil {
		return nil, err
	}

	if err := yaml.DecodeString(file.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadYamlFieldsTo tagged fields in the target struct.
func (l *loader) ReadYamlFieldsTo(ctx context.Context, def *filesystem.FileDef, target any, tag string) (*filesystem.YamlFile, bool, error) {
	if fields := reflecthelper.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadYamlFile(ctx, def); err == nil {
			reflecthelper.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadYamlMapTo tagged field in the target struct as ordered map.
func (l *loader) ReadYamlMapTo(ctx context.Context, def *filesystem.FileDef, target any, tag string) (*filesystem.YamlFile, bool, error) {
	if field := reflecthelper.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadYamlFile(ctx, def); err == nil {
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

// ReadJsonnetFile as AST.
func (l *loader) ReadJsonnetFile(ctx context.Context, def *filesystem.FileDef) (*filesystem.JsonnetFile, error) {
	file, err := l.loadFile(ctx, def, filesystem.FileTypeJsonnet)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JsonnetFile), nil
}

// ReadJsonnetFileTo the target struct.
func (l *loader) ReadJsonnetFileTo(ctx context.Context, def *filesystem.FileDef, target any) (*filesystem.JsonnetFile, error) {
	jsonnetFile, err := l.ReadJsonnetFile(ctx, def)
	if err != nil {
		return nil, formatFileError(def, err)
	}

	jsonFile, err := jsonnetFile.ToJSONRawFile()
	if err != nil {
		return nil, formatFileError(def, err)
	}

	if err := json.DecodeString(jsonFile.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return jsonnetFile, nil
}

// ReadSubDirs filter out ignored directories.
func (l *loader) ReadSubDirs(ctx context.Context, fs filesystem.Fs, root string) ([]string, error) {
	subDirs, err := filesystem.ReadSubDirs(ctx, fs, root)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, subDir := range subDirs {
		isIgnored, err := l.IsIgnored(ctx, filesystem.Join(root, subDir))
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
func (l *loader) IsIgnored(ctx context.Context, path string) (bool, error) {
	if !l.fs.IsDir(ctx, path) {
		return false, nil
	}
	fileDef := filesystem.NewFileDef(filesystem.Join(path, KbcDirFileName))
	fileDef.AddTag(`json`) // `json` is a constant model.FileTypeJSON but cannot be imported due to cyclic imports, it will be refactored

	file, err := l.ReadJSONFile(ctx, fileDef)
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

func (l *loader) loadFile(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
	if l.handler != nil {
		return l.handler(ctx, def, fileType, l.defaultHandler)
	}
	return l.defaultHandler(ctx, def, fileType)
}

func (l *loader) defaultHandler(ctx context.Context, def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
	// Load
	rawFile, err := l.fs.ReadFile(ctx, def)
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
	case filesystem.FileTypeJsonnet:
		return rawFile.ToJSONNetFile(l.jsonnetContext)
	default:
		panic(errors.Errorf(`unexpected filesystem.FileType = %v`, fileType))
	}
}
