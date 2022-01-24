package fileloader

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// Handler is a callback that loads file by definition.
type Handler func(def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error)

// loader implements filesystem.FileLoader.
type loader struct {
	handler Handler
}

func HandlerFromFs(fs filesystem.Fs) Handler {
	return func(def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
		// Load
		rawFile, err := fs.ReadFile(def)
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
			return rawFile.ToJsonNetFile()
		default:
			panic(fmt.Errorf(`unexpected filesystem.FileType = %v`, fileType))
		}
	}
}

func New(handler Handler) filesystem.FileLoader {
	return &loader{handler: handler}
}

func (l *loader) ReadRawFile(def *filesystem.FileDef) (*filesystem.RawFile, error) {
	file, err := l.handler(def, filesystem.FileTypeRaw)
	if err != nil {
		return nil, err
	}

	// Convert to RawFile
	if f, ok := file.(*filesystem.RawFile); ok {
		return f, nil
	}
	return file.ToRawFile()
}

// ReadJsonFile to ordered map.
func (l *loader) ReadJsonFile(def *filesystem.FileDef) (*filesystem.JsonFile, error) {
	file, err := l.handler(def, filesystem.FileTypeJson)
	if err != nil {
		return nil, err
	}

	// Convert to JsonFile
	if f, ok := file.(*filesystem.JsonFile); ok {
		return f, nil
	}
	fileRaw, err := file.ToRawFile()
	if err != nil {
		return nil, err
	}
	return fileRaw.ToJsonFile()
}

// ReadJsonNetFile to AST.
func (l *loader) ReadJsonNetFile(def *filesystem.FileDef) (*filesystem.JsonNetFile, error) {
	file, err := l.handler(def, filesystem.FileTypeJsonNet)
	if err != nil {
		return nil, err
	}

	// Convert to JsonNetFile
	if f, ok := file.(*filesystem.JsonNetFile); ok {
		return f, nil
	}
	fileRaw, err := file.ToRawFile()
	if err != nil {
		return nil, err
	}
	return fileRaw.ToJsonNetFile()
}

// ReadJsonFileTo to target struct.
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

// ReadJsonNetFileTo to target struct.
func (l *loader) ReadJsonNetFileTo(def *filesystem.FileDef, target interface{}) (*filesystem.JsonNetFile, error) {
	file, err := l.ReadJsonNetFile(def)
	if err != nil {
		return nil, formatFileError(def, err)
	}

	jsonContent, err := jsonnet.EvaluateAst(file.Content)
	if err != nil {
		return nil, formatFileError(def, err)
	}

	if err := json.DecodeString(jsonContent, target); err != nil {
		return nil, formatFileError(def, err)
	}

	return file, nil
}

// ReadJsonFieldsTo target struct by tag.
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

// ReadJsonMapTo tagged field in target struct as ordered map.
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

func formatFileError(def *filesystem.FileDef, err error) error {
	fileDesc := strings.TrimSpace(def.Description() + " file")
	return utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", fileDesc, def.Path()), err)
}
