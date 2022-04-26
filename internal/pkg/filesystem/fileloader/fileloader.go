package fileloader

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
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

func (l *loader) SetJsonNetContext(ctx *jsonnet.Context) {
	l.jsonNetContext = ctx
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
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadRawFile(def); err == nil {
			content := strings.TrimRight(file.Content, " \r\n\t")
			utils.SetField(field, content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadJsonFile as an ordered map.
func (l *loader) ReadJsonFile(def *filesystem.FileDef) (*filesystem.JsonFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeJson)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JsonFile), nil
}

// ReadJsonFileTo to the target struct.
func (l *loader) ReadJsonFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.RawFile, error) {
	file, err := l.ReadRawFile(def)
	if err != nil {
		return nil, err
	}

	if err := json.DecodeString(file.Content, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadJsonFieldsTo tagged fields in the target struct.
func (l *loader) ReadJsonFieldsTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	if fields := utils.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadJsonFile(def); err == nil {
			utils.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadJsonMapTo tagged field in the target struct as ordered map.
func (l *loader) ReadJsonMapTo(def *filesystem.FileDef, target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadJsonFile(def); err == nil {
			utils.SetField(field, file.Content, target)
			return file, true, nil
		} else {
			// Set empty map if error occurred
			utils.SetField(field, orderedmap.New(), target)
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadJsonNetFile as AST.
func (l *loader) ReadJsonNetFile(def *filesystem.FileDef) (*filesystem.JsonNetFile, error) {
	file, err := l.loadFile(def, filesystem.FileTypeJsonNet)
	if err != nil {
		return nil, err
	}
	return file.(*filesystem.JsonNetFile), nil
}

// ReadJsonNetFileTo the target struct.
func (l *loader) ReadJsonNetFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.JsonNetFile, error) {
	jsonNetFile, err := l.ReadJsonNetFile(def)
	if err != nil {
		return nil, formatFileError(def, err)
	}

	jsonFile, err := jsonNetFile.ToJsonRawFile()
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
		// Ignore dirs marked with isIgnored flag
		fileDef := filesystem.NewFileDef(filesystem.Join(root, subDir, KbcDirFileName))
		// fileDef.AddTag(model.FileTypeJson)
		if l.fs.Exists(fileDef.Path()) {
			file, err := l.ReadJsonFile(fileDef)
			if err != nil {
				return nil, err
			}
			isIgnored, found := file.Content.Get(KbcDirIsIgnored)
			if found {
				if isIgnored.(bool) {
					continue
				}
			}
		}
		res = append(res, subDir)
	}
	return res, nil
}

func formatFileError(def *filesystem.FileDef, err error) error {
	fileDesc := strings.TrimSpace(def.Description() + " file")
	return utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", fileDesc, def.Path()), err)
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
	case filesystem.FileTypeJson:
		return rawFile.ToJsonFile()
	case filesystem.FileTypeJsonNet:
		return rawFile.ToJsonNetFile(l.jsonNetContext)
	default:
		panic(fmt.Errorf(`unexpected filesystem.FileType = %v`, fileType))
	}
}
